#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
python3 - "$repo/ops/conduit/bin/polyedge-funded-maintenance-failure-proof" <<'PY'
import copy, datetime, json, pathlib, sys
path=pathlib.Path(sys.argv[1]); module={"__name__":"test"}
exec(compile(path.read_text(),str(path),"exec"),module)
invocation="a"*32;container="b"*64
def record(event, timestamp, partial=False):
    row={"_SYSTEMD_INVOCATION_ID":invocation,"CONTAINER_ID_FULL":container,"__REALTIME_TIMESTAMP":str(int(timestamp*1e6)),"MESSAGE":event if isinstance(event,str) else json.dumps(event)}
    if partial:row["CONTAINER_PARTIAL_MESSAGE"]="true"
    return row
result={"schema_version":1,"status":"nothing_to_redeem","dry_run":False,"redemption_enabled":True,"research_only":False,"live_strategy_enabled":True,"redemption_submitted":False,"zero_open_orders_confirmed":True,"selection":{"selected":[],"selected_gross_payout":0},"planned_calls":[],"run_id":"venue-redemption-20260917034156674-5b34d7f3","finished_ts":"1970-01-01T00:14:59Z"}
wrapper={"schema":"polyedge.funded_redemption_service.v1","status":"redemption_worker_summary","redemption":result}
body=json.dumps(wrapper);half=len(body)//2
rows=[record(body[:half],900,True),record(body[half:],900.1),record({"schema":"polyedge.funded_direct_alert.v1","status":"automatic_redemption_failed_closed","error":"fail closed: websocket reconnect reconciliation did not prove a coherent account"},901),record({"schema":"polyedge.funded_direct_service.v2","status":"persistent_service_heartbeat","redemption_failures":1},980)]
prove=lambda records:module["prove"](records,invocation,container,950,1000)
assert prove(rows)["failureCount"]==1
bad=[]
changed=copy.deepcopy(rows);changed[0]["MESSAGE"]=changed[0]["MESSAGE"].replace('"dry_run": false','"dry_run": true');bad.append(changed)
bad.append(rows[1:])
changed=copy.deepcopy(rows);changed[2]["MESSAGE"]=json.dumps({"schema":"polyedge.funded_direct_alert.v1","status":"automatic_redemption_failed_closed","error":"unknown"});bad.append(changed)
changed=copy.deepcopy(rows);changed[-1]["MESSAGE"]=changed[-1]["MESSAGE"].replace('"redemption_failures": 1','"redemption_failures": 2');bad.append(changed)
changed=copy.deepcopy(rows);changed[2]["__REALTIME_TIMESTAMP"]="960000000";bad.append(changed)
bad.append(rows[:2]+[record({"status":"intervening_event"},900.5)]+rows[2:])
bad.append(rows[:2]+[record("intervening non-JSON error",900.5)]+rows[2:])
bad.append(rows[:1])
for records in bad:
    try:prove(records)
    except (AssertionError,ValueError,KeyError):pass
    else:raise AssertionError("unsafe maintenance history accepted")
print("maintenance history fragment and failure-binding checks passed")
PY
