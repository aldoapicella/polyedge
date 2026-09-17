#!/usr/bin/env bash
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
python3 - "$repo/ops/conduit/bin/polyedge-funded-oci-queue-snapshot" <<'PY'
import contextlib, copy, io, json, pathlib, stat, sys, types
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
module["bridge_binding"]=lambda stopped=False:binding
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

# Stopped capture reads one configured public URL and refuses an active process,
# an alternate env file, duplicate values, and mutable configuration permissions.
unit_state='inactive'; main_pid='0'; mode=0o600
quadlet='EnvironmentFile=/etc/polyedge/funded-signer.env\n'
env='PRIVATE_OTHER_VALUE=must-never-appear\nFUNDED_DIRECT_OCI_QUEUE_BRIDGE_URL=http://10.89.0.1:8182/v1/messages\n'
def local_output(*args):
    assert args[0]=='/usr/bin/systemctl'
    return unit_state if args[-2]=='ActiveState' else main_pid
def text_config(path): return quadlet if str(path).endswith('.container') else env
with patch.dict(module,{'output':local_output}), patch.object(pathlib.Path,'read_text',text_config), patch.object(pathlib.Path,'lstat',side_effect=lambda:types.SimpleNamespace(st_mode=stat.S_IFREG|mode,st_uid=0,st_gid=0,st_nlink=1,st_size=128)):
    assert module['stopped_consumer_url']()=={'url':'http://10.89.0.1:8182/v1/messages'}
    for key,bad in [('unit_state','active'),('main_pid','123'),('mode',0o644),('quadlet','EnvironmentFile=/different\n'),('env',env+env)]:
        previous=globals()[key];globals()[key]=bad
        try: module['stopped_consumer_url']()
        except AssertionError: pass
        else: raise AssertionError('unsafe stopped configuration accepted: '+key)
        globals()[key]=previous
print("funded OCI queue read-only snapshot tests passed")
PY
