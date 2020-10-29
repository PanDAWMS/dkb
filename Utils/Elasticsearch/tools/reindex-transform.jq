.hits.hits[]
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
     "conditions_tag": ._source.conditions_tags,
     "ctag_format_step":
      (._source as $src | $src.output_formats
       | map( . + ":" + $src.ctag))
    } + ._source | del(.conditions_tags)
    | del(.output)
