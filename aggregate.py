import sys
import json

data = json.load(sys.stdin)


# Tags that propagate from a segment to individual spans
PROPAGATION_TAGS = ["release", "environment", "transaction"]

# Tags that should be indexed
INDEXED_TAGS = ["db.operation", "db.statement"] + PROPAGATION_TAGS

# Measurement that get indexed
INDEXED_MEASUREMENTS = ["db.rows_returned", "duration"]

aggregator = {}


def clean(description):
    return description


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

        merged_tags = dict(span["tags"])
        if segment is not None:
            counter_key = span["tags"]["op"] + ".counter"
            segment_measurements = segment.setdefault("measurements", {})
            segment_measurements[counter_key] = (
                segment_measurements.get(counter_key) or 0
            ) + 1

            for tag in PROPAGATION_TAGS:
                if tag in segment["tags"]:
                    merged_tags.setdefault(tag, segment["tags"][tag])

        for tag in INDEXED_TAGS:
            if tag in merged_tags:
                tags[tag] = merged_tags[tag]

        for key in INDEXED_MEASUREMENTS:
            if key not in span["measurements"]:
                continue
            aggregator_key = (key, tuple(sorted(tags.items())))
            aggregator.setdefault(aggregator_key, []).append(span["measurements"][key])


data["metrics"] = [
    {"metric": x[0][0], "tags": dict(sorted(x[0][1])), "values": x[1]}
    for x in sorted(aggregator.items())
]
print(json.dumps(data, indent=2))
