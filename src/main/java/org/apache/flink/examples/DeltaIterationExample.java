package org.apache.flink.examples;

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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
/**
 * Simplified version of org.apache.flink.examples.java.graph.ConnectedComponents
 * No circular links or two parents allowed. Also making it a directed 
 * instead of an un-directed graph
 * @author Sameer
 *
 */
@SuppressWarnings("serial")
public class DeltaIterationExample {
    private static final int MAX_ITERATIONS = 4;
    public static void main(String... args) throws Exception {
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // read vertex and edge data
        // initially assign parent vertex id== my vertex id
        DataSet<Tuple2<Long, Long>> vertices = GraphData.getDefaultVertexDataSet(env);
        DataSet<Tuple2<Long, Long>> edges = GraphData.getDefaultEdgeDataSet(env);

        int vertexIdIndex = 0;
        // open a delta iteration
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =                
                           vertices.iterateDelta(vertices , MAX_ITERATIONS, vertexIdIndex );                                                 

        // apply the step logic: join with the edges, select the minimum
        // neighbor, update if the component of the candidate is smaller
        DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset().join(edges).where(0).equalTo(0)
        /* Update the parentVertex=parent.id */
        .with(new NeighborWithComponentIDJoin())
        /* Merge with solution set */
        .join(iteration.getSolutionSet()).where(0).equalTo(0)
        /* Only pass on the changes to next iteration */
        .with(new ComponentIdFilter());
        
        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);
        result.print();
    }
  
    /**
     * UDF that joins a (Vertex-ID, Component-ID) pair that represents the
     * current component that a vertex is associated with, with a
     * (Source-Vertex-ID, Target-VertexID) edge. The function produces a
     * (Target-vertex-ID, Component-ID) pair.
     */
    @ForwardedFieldsFirst("f1->f1")
    @ForwardedFieldsSecond("f1->f0")
    public static final class NeighborWithComponentIDJoin implements
            JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
            return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
        }
    }

    @ForwardedFieldsFirst("*")
    public static final class ComponentIdFilter implements
            FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
            if (candidate.f1 < old.f1) {
                out.collect(candidate);
            }
        }
    }
    

}