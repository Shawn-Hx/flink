package org.apache.flink.runtime.scheduler.newscheduler;

import com.alibaba.fastjson.annotation.JSONField;

public class Operator {

	@JSONField
	public int id;
	@JSONField(name = "vertex_id")
	public int vertexId;
	@JSONField(name = "task_id")
	public int taskId;
	@JSONField(name = "is_source")
	public boolean isSource;
	@JSONField(name = "is_sink")
	public boolean isSink;
	@JSONField
	public double cpu;
	@JSONField
	public int memory;

}
