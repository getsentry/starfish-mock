import sys
import json

data = json.load(sys.stdin)


def clean(description):
    return description


PROPAGATION_TAGS = ["release", "environment", "transaction"]
AGGREGATION_KEYS = ["db.operation", "db.statement"]

aggregator = {}

for batch_id, batch in data["batches"].items():
    if batch_id:
        segment = batch[-1]
        assert segment["is_segment"]
    else:
        segment = None

    for span in batch:
        tags = {}
        span.setdefault("measurements", {})["duration"] = (
            span["end_time"] - span["start_time"]
        )
        tags["op"] = span["tags"]["op"]
        tags["cleaned_description"] = clean(span["description"])

        if segment is not None:
            counter_key = span["tags"]["op"] + ".counter"
            segment_measurements = segment.setdefault("measurements", {})
            segment_measurements[counter_key] = (
                segment_measurements.get(counter_key) or 0
            ) + 1

            for aggr in PROPAGATION_TAGS:
                if aggr in segment["tags"]:
                    tags[aggr] = segment["tags"][aggr]

        for aggr in AGGREGATION_KEYS:
            if aggr in span["tags"]:
                tags[aggr] = span["tags"][aggr]

        key = "span.duration"
        aggregator_key = (key, tuple(sorted(tags.items())))
        aggregator.setdefault(aggregator_key, []).append(
            span["measurements"]["duration"]
        )


import json

data["metrics"] = [
    {"metric": x[0][0], "tags": dict(sorted(x[0][1])), "values": x[1]}
    for x in sorted(aggregator.items())
]
print(json.dumps(data, indent=2))
