/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.LongValue;

@SuppressWarnings("serial")
public class AdderBulkIterationsWrongWay implements java.io.Serializable {
    /*Each input number must be between 0 - INPUT_MAX (exclusive)*/
    public static int INPUT_MAX = 100;
    
    /*Number of elements in our toy dataset*/
    public static int NO_OF_ELEMENTS = 10;
    
    /*Iterations stop when sum of all numbers exceeds ABSOLUTE_MAX*/
    public static long ABSOLUTE_MAX = INPUT_MAX * NO_OF_ELEMENTS;

    /*Maxium Iterations*/
    public static int MAX_ITERATIONS = 100000;
    
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Long, Long>> input = getData(env);
        long initialTime = System.currentTimeMillis();
        
        DataSet<Tuple2<Long, Long>> output = input;
        
        for(int i=0;i<MAX_ITERATIONS;i++){
            output = input.map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                        @Override
                        public Tuple2<Long, Long> map(
                                Tuple2<Long, Long> input) throws Exception {
                            long incrementF1 = input.f1 + 1;                            
                            Tuple2<Long, Long> out = new Tuple2<>(input.f0, incrementF1);
                            return out;
                        }
                    });
            
            long sum = output.map(new FixTuple2()).reduce(new ReduceFunc()).collect().get(0);
            input = output;
            System.out.println("Current Sum="+sum);            
            if(sum>ABSOLUTE_MAX){
                System.out.println("Breaking now:"+i);
                break;
            }

        }        
        output.print();
        System.out.println("Total time to run the job " + (System.currentTimeMillis()-initialTime));
    }

    public static DataSet<Tuple2<Long, Long>> getData(
            ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> lst = new ArrayList<Tuple2<Long, Long>>();
        Random rnd = new Random();
        for (int i = 0; i < NO_OF_ELEMENTS; i++) {
            long r = rnd.nextInt(INPUT_MAX);
            lst.add(new Tuple2<Long, Long>(r, r));
        }
        return env.fromCollection(lst);
    }
    
    public static class VerifyIfMaxConvergence implements ConvergenceCriterion<LongValue>{
        @Override
        public boolean isConverged(int iteration, LongValue value) {
            return (value.getValue()>AdderBulkIterationsWrongWay.ABSOLUTE_MAX);
        }
    }
    
    public static class FixTuple2 implements MapFunction<Tuple2<Long,Long>,Long>{
        @Override
        public Long map(Tuple2<Long, Long> value) throws Exception {
            return value.f1;
        }        
    }
    
    public static class ReduceFunc implements ReduceFunction<Long>{
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1+value2;
        }
        
    }
}