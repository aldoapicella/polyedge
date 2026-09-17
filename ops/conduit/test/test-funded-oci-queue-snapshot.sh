#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
python3 - "$repo/ops/conduit/bin/polyedge-funded-oci-queue-snapshot" <<'PY'
import contextlib, copy, io, json, pathlib, sys, types
from unittest.mock import patch
path=pathlib.Path(sys.argv[1]); module={"__name__":"test", "__file__":str(path)}
exec(compile(path.read_text(),str(path),"exec"),module)
snapshot=module["snapshot"]
zero={name:{key:0 for key in ("visible_messages","in_flight_messages","size_in_bytes")} for name in ("queue","dlq")}
objects=[{"name":"funded-queue-dlq/2026/09/17/one-"+"a"*64+".json","etag":"one","size":807},
         {"name":"funded-queue-dlq/2026/09/17/two-"+"b"*64+".json","etag":"two","size":901}]
binding={"queueId":"queue", "objectNamespace":"namespace", "dlqBucket":"bucket", "endpoint":"https://queue"}
value=snapshot(zero,zero,objects,binding)
assert value["archiveDlq"]["objectCount"]==2 and "scheduledMessageCount" not in value
assert snapshot(zero,zero,list(reversed(objects)),binding)==value
assert snapshot(zero,zero,objects[:1],binding)["archiveDlq"]["inventorySha256"]!=value["archiveDlq"]["inventorySha256"]
for location in ("queue","dlq"):
    for field in zero[location]:
        for invalid in (1,-1,None,False,"0"):
            bad=copy.deepcopy(zero);bad[location][field]=invalid
            for args in ((bad,zero,objects,binding),(zero,bad,objects,binding)):
                try: snapshot(*args)
                except AssertionError: pass
                else: raise AssertionError("invalid native counts accepted")
for bad in (objects+[objects[0]], [{**objects[0],"etag":None}], [{**objects[0],"name":"other/payload"}]):
    try: snapshot(zero,zero,bad,binding)
    except AssertionError: pass
    else: raise AssertionError("invalid or overlapping archive accepted")

# The SDK mock exposes only read-only operations. Main must page the metadata
# inventory and repeat both global and channel stats without receiving a message.
reads=[]; pages=[]; wrong_channel=False
class Queue:
    def get_stats(self,queue_id,**kwargs):
        assert queue_id==binding["queueId"];reads.append(kwargs)
        return types.SimpleNamespace(data=types.SimpleNamespace(channel_id="wrong" if wrong_channel else kwargs.get("channel_id"),
            **{key:types.SimpleNamespace(**row) for key,row in zero.items()}))
class Storage:
    def list_objects(self,namespace,bucket,**kwargs):
        assert (namespace,bucket)==("namespace","bucket") and kwargs["prefix"]=="funded-queue-dlq/"
        pages.append(kwargs.get("start"));index=0 if kwargs.get("start") is None else 1
        return types.SimpleNamespace(data=types.SimpleNamespace(objects=[types.SimpleNamespace(**objects[index])],next_start_with="next" if index==0 else None))
oci=types.SimpleNamespace(auth=types.SimpleNamespace(signers=types.SimpleNamespace(InstancePrincipalsSecurityTokenSigner=lambda:object())),
    retry=types.SimpleNamespace(NoneRetryStrategy=lambda:object()),queue=types.SimpleNamespace(QueueClient=lambda *a,**kw:Queue()),
    object_storage=types.SimpleNamespace(ObjectStorageClient=lambda *a,**kw:Storage()))
module["bridge_binding"]=lambda:binding
with patch.dict(sys.modules,{"oci":oci}), patch.object(module["os"],"geteuid",return_value=1000), patch.object(module["pwd"],"getpwnam",return_value=types.SimpleNamespace(pw_uid=1000)):
    cloud=module["read_cloud"](binding)
    wrong_channel=True
    try: module["read_cloud"](binding)
    except AssertionError: pass
    else: raise AssertionError("wrong response channel accepted")
reads.pop()
assert reads==[{}, {"channel_id":"funded-direct"}, {}, {"channel_id":"funded-direct"}] and pages==[None,"next"]
with patch.object(module["os"],"geteuid",return_value=0):
    try: module["read_cloud"](binding)
    except AssertionError: pass
    else: raise AssertionError("root imported the mutable SDK")
def worker(command, **kwargs):
    assert command==["/usr/sbin/runuser","-u","ubuntu","--","/home/ubuntu/.local/share/oci-cli/bin/python3","-I",str(path.resolve()),"--read"]
    assert json.loads(kwargs["input"])==binding and kwargs["check"] and kwargs["timeout"]==120
    assert set(kwargs["env"])=={"PATH","LANG"}
    return types.SimpleNamespace(stdout=json.dumps(cloud))
with patch.object(module["os"],"geteuid",return_value=0), patch.object(module["subprocess"],"run",side_effect=worker), contextlib.redirect_stdout(io.StringIO()) as captured:
    module["main"]()
result=json.loads(captured.getvalue())
assert result["archiveDlq"]==value["archiveDlq"] and result["verifier"]["path"]==str(path.resolve())
assert result["status"]=="observed_zero" and result["statisticsAreApproximate"] is True
print("funded OCI queue read-only snapshot tests passed")
PY
