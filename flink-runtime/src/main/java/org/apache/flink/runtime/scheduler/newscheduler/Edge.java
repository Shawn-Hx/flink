package org.apache.flink.runtime.scheduler.newscheduler;

import com.alibaba.fastjson.annotation.JSONField;

public class Edge {

	@JSONField(name = "from_id")
	public int fromId;
	@JSONField(name = "to_id")
	public int toId;

}
