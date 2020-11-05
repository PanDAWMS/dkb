def input_events_v2:
  if .taskid == .chain_id then
    if .total_req_events then
      .total_req_events
    else
      if .primary_input_events then
        .primary_input_events
      else
        .requested_events
      end
    end
  else
    if (.parent_task_name|tostring|contains(".deriv.")|not) then
      .parent_total_events
    else
      .requested_events
    end
  end;

def processed_events_v2:
  if (.input_events_v2 and .processed_events and .requested_events and .requested_events > 0) then
    ((.input_events_v2|tonumber)
     * (.processed_events|tonumber)
     / (.requested_events|tonumber)) | round
  else
    if (.step_name|tostring|ascii_downcase) == "evgen" then
      .total_events
    else
      null
    end
  end;

.hits.hits[]
  | ._source as $task
  | last((._source.taskname / ".")[]) as $ami_tags
  | ($task | input_events_v2) as $input_events_v2
  | (($task + {"input_events_v2": $input_events_v2})
     | processed_events_v2) as $processed_events_v2
  | {"index":
      {"_index": $TGT_INDEX,
       "_type": "task",
       "_id": ._id}
    },
    {"output_dataset":
      .inner_hits.datasets.hits.hits
      | map(._source | .name = .datasetname
            | .data_format = .data_format[0]
            | del(.datasetname)),
     "conditions_tag": $task.conditions_tags,
     "ctag_format_step":
      ($task.output_formats
       | map( . + ":" + $task.ctag)),
     "ami_tags": $ami_tags,
     "ami_tags_format_step":
      ($task.output_formats
       | map(. + ":" + $ami_tags)),
     "input_events_v2": $input_events_v2,
     "processed_events_v2": $processed_events_v2
    } + $task
    | del(.conditions_tags, .output, .job_config, .evgen_job_opts)
