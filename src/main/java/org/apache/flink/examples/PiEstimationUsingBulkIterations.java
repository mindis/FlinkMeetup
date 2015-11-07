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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

/** 
 * Estimates the value of Pi using the Monte Carlo method.
 * The area of a circle is Pi * R^2, R being the radius of the circle 
 * The area of a square is 4 * R^2, where the length of the square's edge is 2*R.
 * 
 * Thus Pi = 4 * (area of circle / area of square).
 * 
 * The idea is to find a way to estimate the circle to square area ratio.
 * The Monte Carlo method suggests collecting random points (within the square)
 * and then counting the number of points that fall within the circle
 * 
 * <pre>
 * {@code
 * x = Math.random()
 * y = Math.random()
 * 
 * x * x + y * y < 1
 * }
 * </pre>
 */
@SuppressWarnings("serial")
public class PiEstimationUsingBulkIterations implements java.io.Serializable {
    
    public static int NO_OF_ITERATIONS=100000;
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        //First create an initial dataset
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(NO_OF_ITERATIONS);

        DataSet<Integer> iteration = initial.map(new MapFunction<Integer,Integer>(){
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return i + ((x*x+y*y<1) ? 1 : 0);
            }            
        });        
        DataSet<Integer> count = initial.closeWith(iteration);
        long theCount = count.collect().get(0);
        System.out.println("We estimate Pi to be: " + (theCount * 4.0 / NO_OF_ITERATIONS));
    }
}