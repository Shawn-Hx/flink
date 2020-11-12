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

package org.apache.flink.runtime.scheduler.newscheduler;

/**
 * New scheduler utilities.
 */
public class Util {

	public static final String HOME_DIR = System.getProperty("user.home");

	public static final String LINE_SPLITTER = System.getProperty("line.separator");

	public static final String DSP_GRAPH_FILE = HOME_DIR + "/Desktop/flink_graph.json";

	public static final String RESOURCE_FILE = HOME_DIR + "/Desktop/slots.json";

	public static final String PLACEMENT_FILE = HOME_DIR + "/Desktop/placement.json";

	public static final String PYTHON = HOME_DIR + "/gitProjects/DRL-Scheduler/venv/bin/python";

	public static final String SCRIPT_FILE = HOME_DIR + "/gitProjects/DRL-Scheduler/eval.py";

//	public static final String MODEL_FILE = "";

}
