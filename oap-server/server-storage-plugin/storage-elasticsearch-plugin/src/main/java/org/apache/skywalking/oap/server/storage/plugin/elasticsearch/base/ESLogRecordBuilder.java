/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.oap.server.core.analysis.manual.log.AbstractLogRecord;
import org.apache.skywalking.oap.server.core.analysis.manual.log.LogRecord;
import org.apache.skywalking.oap.server.core.query.type.ContentType;

import java.util.Map;

public class ESLogRecordBuilder extends LogRecord.Builder {

    Gson gson = new Gson();

    @Override
    public LogRecord storage2Entity(Map<String, Object> dbMap) {

        int contentType = ((Number) dbMap.get(AbstractLogRecord.CONTENT_TYPE)).intValue();
        if (contentType == ContentType.JSON.value()) {
            Object content = dbMap.get(AbstractLogRecord.CONTENT + "_json");

            if (content != null) {
                String s = gson.toJson(content);
                dbMap.put(AbstractLogRecord.CONTENT, s);
            }
        }

        return super.storage2Entity(dbMap);
    }

    @Override
    public Map<String, Object> entity2Storage(LogRecord record) {
        Map<String, Object> stringObjectMap = super.entity2Storage(record);

        int contentType = (int) stringObjectMap.get(AbstractLogRecord.CONTENT_TYPE);

        if (contentType == ContentType.JSON.value()) {
            String content = (String) stringObjectMap.get(AbstractLogRecord.CONTENT);

            if (StringUtils.isNotEmpty(content)) {
                Map bean = gson.fromJson(content, Map.class);
                stringObjectMap.put(AbstractLogRecord.CONTENT + "_json", bean);
                //keep compatible
                stringObjectMap.put(AbstractLogRecord.CONTENT, "{}");
            }
        }

        return stringObjectMap;
    }
}
