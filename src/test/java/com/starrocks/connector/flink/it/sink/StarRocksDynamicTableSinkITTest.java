/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.it.sink;

import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

import mockit.Expectations;

import static org.junit.Assert.assertFalse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.starrocks.connector.flink.StarRocksSinkBaseTest;

public class StarRocksDynamicTableSinkITTest extends StarRocksSinkBaseTest {

    @Test
    public void testMultiThreadFlush() throws Exception {
        mockStarRocksVersion(null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1000000);
        env.enableCheckpointing(30 * 1000L);
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", JDBC_URL)
                .withProperty("load-url", LOAD_URL)
                .withProperty("database-name", DATABASE)
                .withProperty("table-name", TABLE)
                .withProperty("username", USERNAME)
                .withProperty("password", PASSWORD)
                .withProperty("sink.label-prefix", SINK_LABEL_PREFIX)
                .withProperty("sink.semantic", SINK_SEMANTIC.getName())
                .withProperty("sink.buffer-flush.interval-ms", SINK_MAX_INTERVAL)
                .withProperty("sink.buffer-flush.max-bytes", SINK_MAX_BYTES)
                .withProperty("sink.buffer-flush.max-rows", SINK_MAX_ROWS)
                .withProperty("sink.max-retries", SINK_MAX_RETRIES)
                // .withProperty("sink.flush-thread", SINK_FLUSH_THREAD)
                .withProperty("sink.connect.timeout-ms", "2000");

        env.addSource(new DataGeneratorSource<>(SequenceGenerator.intGenerator(0, 10000) ,200, null)).returns(Integer.class)
                .map(new MapFunction<Integer, StarRocksSinkRowDataWithMeta>() {
                    @Override
                    public StarRocksSinkRowDataWithMeta map(Integer s) throws Exception {
                        StarRocksSinkRowDataWithMeta starRocksSinkRowDataWithMeta = new StarRocksSinkRowDataWithMeta();
                        starRocksSinkRowDataWithMeta.setDatabase("db_" + new Random().nextInt(5));
                        starRocksSinkRowDataWithMeta.setTable("table_" + new Random().nextInt(10));
                        starRocksSinkRowDataWithMeta.addDataRow(s.toString());
                        return starRocksSinkRowDataWithMeta;
                    }
                })
                .addSink(new StarRocksDynamicSinkFunction<StarRocksSinkRowDataWithMeta>(builder.build())).setParallelism(10);
        env.execute();
    }

    @Test
    public void sum(){
        //dataSum: 50005000
        // dataRowCounts:10001
        int sum = 0;
        for (int i = 0; i < 10001; i++) {
            sum += i;
        }
        System.out.println(sum);
    }
    @Test
    public void testBatchSink() {
        List<Map<String, String>> meta = new ArrayList<>();
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "name");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "varchar");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "score");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "bigint");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "a");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "varchar");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "e");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "array");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "f");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "array");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "g");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "array");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "h");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "array");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "i");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "map");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "j");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "map");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "k");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "map");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "d");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "date");
        }});
        meta.add(new HashMap<String, String>(){{
            put("COLUMN_NAME", "t");
            put("COLUMN_KEY", "");
            put("DATA_TYPE", "datetime");
        }});
        new Expectations(){
            {
                v.getTableColumnsMetaData();
                result = meta;
            }
        };

        mockStarRocksVersion(null);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
            .useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(bsSettings);
        mockSuccessResponse();
        String createSQL = "CREATE TABLE USER_RESULT(" +
            "name VARCHAR," +
            "score BIGINT," +
            "t TIMESTAMP(3)," +
            "a ROW<k1 int, k2 string>," +
            "e ARRAY<ROW<k1 int, k2 string>>," +
            "f ARRAY<STRING>," +
            "g ARRAY<DECIMAL(2,1)>," +
            "h ARRAY<ARRAY<STRING>>," +
            "i MAP<STRING,INT>," +
            "j MAP<STRING, MAP<STRING,INT>>," +
            "k MAP<STRING, ARRAY<INT>>," +
            "d DATE" +
            ") WITH ( " +
            "'connector' = 'starrocks'," +
            "'jdbc-url'='" + OPTIONS.getJdbcUrl() + "'," +
            "'load-url'='" + String.join(";", OPTIONS.getLoadUrlList()) + "'," +
            "'database-name' = '" + OPTIONS.getDatabaseName() + "'," +
            "'table-name' = '" + OPTIONS.getTableName() + "'," +
            "'username' = '" + OPTIONS.getUsername() + "'," +
            "'password' = '" + OPTIONS.getPassword() + "'," +
            "'sink.buffer-flush.max-rows' = '" + OPTIONS.getSinkMaxRows() + "'," +
            "'sink.buffer-flush.max-bytes' = '" + OPTIONS.getSinkMaxBytes() + "'," +
            "'sink.buffer-flush.interval-ms' = '" + OPTIONS.getSinkMaxFlushInterval() + "'," +
            "'sink.buffer-flush.enqueue-timeout-ms' = '" + OPTIONS.getSinkOfferTimeout() + "'," +
            // "'sink.properties.format' = 'json'," +
            // "'sink.properties.strip_outer_array' = 'true'," +
            "'sink.properties.column_separator' = '\\x01'," +
            "'sink.properties.row_delimiter' = '\\x02'" +
            ")";
        tEnv.executeSql(createSQL);

        String exMsg = "";
        try {
            tEnv.executeSql("INSERT INTO USER_RESULT\n" +
                "VALUES ('lebron', 99, TO_TIMESTAMP('2020-01-01 01:00:01'), row(1,'a'), array[row(1,'a')], array['accc','bccc'], array[1.2,2.3], array[array['1','2']], map['k1', 222, 'k2', 111], map['nested', map['k1', 222, 'k2', 111]], map['nested', array[1, 3]], TO_DATE('2020-01-01'))").collect();
            Thread.sleep(2000);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertFalse(exMsg, exMsg.length() > 0);
    }
}
