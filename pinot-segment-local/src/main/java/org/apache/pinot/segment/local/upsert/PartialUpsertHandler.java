/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMerger;
import org.apache.pinot.segment.local.upsert.merger.PartialUpsertMergerFactory;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for partial-upsert.
 */
public class PartialUpsertHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartialUpsertHandler.class);

  // _column2Mergers maintains the mapping of merge strategies per columns.
  private final Map<String, PartialUpsertMerger> _column2Mergers = new HashMap<>();

  private final HelixManager _helixManager;
  private final String _tableNameWithType;
  private boolean _allSegmentsLoaded;

  public PartialUpsertHandler(HelixManager helixManager, String tableNameWithType, Schema schema,
      Map<String, UpsertConfig.Strategy> partialUpsertStrategies, UpsertConfig.Strategy defaultPartialUpsertStrategy,
      String comparisonColumn) {
    _helixManager = helixManager;
    _tableNameWithType = tableNameWithType;
    for (Map.Entry<String, UpsertConfig.Strategy> entry : partialUpsertStrategies.entrySet()) {
      _column2Mergers.put(entry.getKey(), PartialUpsertMergerFactory.getMerger(entry.getValue()));
    }
    // For all physical columns (including date time columns) except for primary key columns and comparison column.
    // If no comparison column is configured, use main time column as the comparison time.
    for (String columnName : schema.getPhysicalColumnNames()) {
      if (!schema.getPrimaryKeyColumns().contains(columnName) && !_column2Mergers.containsKey(columnName)
          && !comparisonColumn.equals(columnName)) {
        _column2Mergers.put(columnName, PartialUpsertMergerFactory.getMerger(defaultPartialUpsertStrategy));
      }
    }
  }

  /**
   * Returns {@code true} if all segments assigned to the current instance are loaded, {@code false} otherwise.
   * Consuming segment should perform this check to ensure all previous records are loaded before inserting new records.
   */
  public synchronized boolean isAllSegmentsLoaded() {
    if (_allSegmentsLoaded) {
      return true;
    }

    HelixDataAccessor dataAccessor = _helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = dataAccessor.keyBuilder();
    IdealState idealState = dataAccessor.getProperty(keyBuilder.idealStates(_tableNameWithType));
    if (idealState == null) {
      LOGGER.warn("Failed to find ideal state for table: {}", _tableNameWithType);
      return false;
    }
    String instanceName = _helixManager.getInstanceName();
    LiveInstance liveInstance = dataAccessor.getProperty(keyBuilder.liveInstance(instanceName));
    if (liveInstance == null) {
      LOGGER.warn("Failed to find live instance for instance: {}", instanceName);
      return false;
    }
    String sessionId = liveInstance.getEphemeralOwner();
    CurrentState currentState =
        dataAccessor.getProperty(keyBuilder.currentState(instanceName, sessionId, _tableNameWithType));
    if (currentState == null) {
      LOGGER.warn("Failed to find current state for instance: {}, sessionId: {}, table: {}", instanceName, sessionId,
          _tableNameWithType);
      return false;
    }

    // Check if ideal state and current state matches for all segments assigned to the current instance
    Map<String, Map<String, String>> idealStatesMap = idealState.getRecord().getMapFields();
    Map<String, String> currentStateMap = currentState.getPartitionStateMap();
    for (Map.Entry<String, Map<String, String>> entry : idealStatesMap.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      String expectedState = instanceStateMap.get(instanceName);
      // Only track ONLINE segments assigned to the current instance
      if (!SegmentStateModel.ONLINE.equals(expectedState)) {
        continue;
      }
      String actualState = currentStateMap.get(segmentName);
      if (!SegmentStateModel.ONLINE.equals(actualState)) {
        if (SegmentStateModel.ERROR.equals(actualState)) {
          LOGGER
              .error("Find ERROR segment: {}, table: {}, expected: {}", segmentName, _tableNameWithType, expectedState);
        } else {
          LOGGER.info("Find unloaded segment: {}, table: {}, expected: {}, actual: {}", segmentName, _tableNameWithType,
              expectedState, actualState);
        }
        return false;
      }
    }

    LOGGER.info("All segments loaded for table: {}", _tableNameWithType);
    _allSegmentsLoaded = true;
    return true;
  }

  /**
   * Merges 2 records and returns the merged record.
   * We used a map to indicate all configured fields for partial upsert. For these fields
   * (1) If the prev value is null, return the new value
   * (2) If the prev record is not null, the new value is null, return the prev value.
   * (3) If neither values are not null, then merge the value and return.
   * For un-configured fields, they are using default override behavior, regardless null values.
   *
   * For example, overwrite merger will only override the prev value if the new value is not null.
   * Null values will override existing values if not configured. They can be ignored by using ignoreMerger.
   *
   * @param previousRecord the last derived full record during ingestion.
   * @param newRecord the new consumed record.
   * @return a new row after merge
   */
  public GenericRow merge(GenericRow previousRecord, GenericRow newRecord) {
    for (Map.Entry<String, PartialUpsertMerger> entry : _column2Mergers.entrySet()) {
      String column = entry.getKey();
      if (!previousRecord.isNullValue(column)) {
        if (newRecord.isNullValue(column)) {
          newRecord.putValue(column, previousRecord.getValue(column));
          newRecord.removeNullValueField(column);
        } else {
          newRecord
              .putValue(column, entry.getValue().merge(previousRecord.getValue(column), newRecord.getValue(column)));
        }
      }
    }

    // there is a case: need reserve previous column value but abort the new value, like metrics create time column
    // currently, there is not have a strategy for this, so we need a temp solution
    reverseNoneUpsertFields(previousRecord, newRecord);
    return newRecord;
  }

  /**
   * reserve previous column value while column without configure upsert strategy
   * @param previousRecord
   * @param newRecord
   */
  private void reverseNoneUpsertFields(GenericRow previousRecord, GenericRow newRecord) {
    Set<String> column2Mergers = _column2Mergers.keySet();
    Set<String> recordColumns = newRecord.getFieldToValueMap().keySet();

    recordColumns.forEach(recordColumn -> {
      if (!column2Mergers.contains(recordColumn)) {
        boolean isPreviousRecordNull = previousRecord.getNullValueFields().contains(recordColumn);
        // use none valued previous column
        if (!isPreviousRecordNull) {
          Object previousValue = previousRecord.getValue(recordColumn);
          newRecord.putValue(recordColumn, previousValue);
          if (newRecord.isNullValue(recordColumn)) {
            newRecord.removeNullValueField(recordColumn);
          }
        }
      }
    });
  }
}
