package org.apache.flink.runtime.scheduler.newscheduler;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.ArrayList;
import java.util.List;

public class DSPGraph {
	@JSONField
	public int id;
	@JSONField(name = "max_parallelism")
	public int maxParallelism;
	@JSONField
	public List<Operator> operators = new ArrayList<>();
	@JSONField
	public List<Edge> edges = new ArrayList<>();
}
