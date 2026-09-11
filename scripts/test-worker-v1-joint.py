#!/usr/bin/env python3
"""Actual Control/Gateway/Worker process gate; only model and Telegram are fixtures."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
sys.dont_write_bytecode = True
sys.path.insert(0,str(Path(__file__).resolve().parent/'worker-v1-joint'))
from harness import Harness,assert_model_contract
import gateway_fixture


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument('--artifacts',type=Path)
    p.add_argument('--race',action='store_true')
    p.add_argument('--model-name',default='joint-fixture',help='model name published through Control Profile; external HTTP remains the explicit local fixture')
    p.add_argument('--faults',action='store_true',help='also run real process SIGKILL recovery cases')
    p.add_argument('--contracts',action='store_true',help='also run Reply recovery, Session interruption and live credential mutation matrices')
    p.add_argument('--recovery',action='store_true',help='also run intake commit/ACK faults, Manifest offline export, SessionScope changes and rejection storage recovery')
    p.add_argument('--durability',action='store_true',help='also run committed Session response loss, cross Tenant/Account isolation and retained Run capacity recovery')
    p.add_argument('--uncertainty',action='store_true',help='also run real credential response/lock faults, fixed deadlines and uncertain Telegram delivery')
    p.add_argument('--authorization',action='store_true',help='also run credential batch rejection and real online proof transport/denial classification')
    p.add_argument('--finality',action='store_true',help='also run valid Final proof identity mismatches and independent reply deadline outcomes')
    p.add_argument('--manifest-rebuild',action='store_true',help='also rebuild only the lost Manifest increment source and recover fixed snapshots from the real Control owner')
    args=p.parse_args()
    artifacts=args.artifacts or Path(tempfile.mkdtemp(prefix='worker-joint-evidence-'))
    h=Harness(args.root,artifacts,args.race,model_name=args.model_name)
    print('JOINT_ARTIFACTS='+str(h.artifacts),flush=True)
    try:
        h.provision()
        if args.durability:
            from commit_scenarios import install_proxy
            install_proxy(h)
        if args.uncertainty or args.authorization or args.finality or args.manifest_rebuild:
            from resolve_scenarios import install_resolve_proxies
            install_resolve_proxies(h)
        h.control_start(gateway_fixture.prepare(h))
        if args.recovery:
            from manifest_scenarios import run_manifest
            run_manifest(h, phase='empty')
        h.seed(extra_tenants=1 if args.durability else 0)
        h.start_worker()
        h.verify_dependencies()
        h.gateway=gateway_fixture.start(h)
        runs=[]
        for text in ('joint round one','joint round two'):
            run=h.send_text(text);h.wait_delivery(run);runs.append(run)
        if len(h.model.requests)!=2:raise RuntimeError('two-round model request count differs')
        published=h.api('GET','/v1/tenants/'+h.tenant_id+'/deployments/'+h.deployment_id+'/revisions/'+str(h.revision_number))
        model_evidence={'selected_model':h.model_name,'manifest_id':h.manifest_id,'manifest_digest':h.manifest_digest,
                        'manifest_view':published['manifest_view'],'model_requests':h.model.requests,'result':'PENDING'}
        model_path=h.artifacts/'joint-model-parameters.json'
        model_path.write_text(json.dumps(model_evidence,indent=2)+'\n')
        model_contract=assert_model_contract(h.model.requests,published['manifest_view'],h.model_name)
        model_evidence.update(result='PASS',comparison=model_contract)
        model_path.write_text(json.dumps(model_evidence,indent=2)+'\n')
        assert json.loads(model_path.read_text())==model_evidence
        history=json.dumps(h.model.requests[1],ensure_ascii=False)
        if 'joint answer: joint round one' not in history:raise RuntimeError('second real SDK request lacks accepted first Session')
        rows=h.sql("SELECT r.run_id,r.status,r.session_sequence,c.completion_id,o.published_at IS NOT NULL FROM worker.execution_runs r JOIN worker.execution_completions c USING(tenant_id,run_id) JOIN worker.execution_reply_outbox o USING(tenant_id,run_id) WHERE r.run_id IN ("+','.join(h.quote(r) for r in runs)+") ORDER BY r.session_sequence")
        if len(rows)!=2 or any(row[1]!='SUCCEEDED' or row[4]!='t' for row in rows):raise RuntimeError('joint Completion/Reply evidence differs: '+repr(rows))
        (h.artifacts/'joint-two-rounds.json').write_text(json.dumps({'runs':rows,'model_requests':h.model.requests,'model_contract':model_contract,'manifest_id':h.manifest_id,'manifest_digest':h.manifest_digest,'external_fixture':['OpenAI-compatible model','Telegram Bot API']},indent=2)+'\n')
        print('WORKER_JOINT_TWO_ROUNDS=PASS actual Control HTTP publication + Manifest Relay/Owner Export + mTLS online proofs + actual Gateway ingress/delivery + actual Worker SDK/Session/Final; explicit model/Telegram fixtures',flush=True)
        if args.manifest_rebuild:
            from manifest_gap_scenarios import run_manifest_gap
            run_manifest_gap(h)
        if args.recovery:
            from intake_scenarios import run_intake
            from scope_scenarios import run_scope
            from receipt_scenarios import run_receipt
            run_intake(h)
            h.start_worker()
            run_manifest(h)
            run_scope(h)
            run_receipt(h)
        if args.durability:
            from commit_scenarios import run_commit
            from identity_scenarios import run_identity
            from retained_scenarios import run_retained
            run_commit(h)
            run_identity(h)
            run_retained(h)
        if args.uncertainty:
            from resolve_scenarios import run_resolve
            from deadline_scenarios import run_deadline
            from delivery_scenarios import run_delivery
            run_resolve(h)
            run_deadline(h)
            run_delivery(h)
        if args.authorization:
            from credential_batch_scenarios import run_credential_batch
            from proof_authorization_scenarios import run_proof_authorization
            run_credential_batch(h)
            run_proof_authorization(h)
        if args.finality:
            from final_identity_scenarios import run_final_identity
            from reply_deadline_scenarios import run_reply_deadline
            run_final_identity(h)
            run_reply_deadline(h)
        if args.contracts:
            from reply_scenarios import run_reply
            run_reply(h)
        webhook = h.gateway.verify_webhook_auth_and_replay(runs[0])
        h.stop_worker()
        replay = h.gateway.restart_and_replay(runs[-1])
        (h.artifacts/'joint-webhook-replay.json').write_text(json.dumps({'webhook':webhook,'reply':replay},indent=2)+'\n')
        print('WORKER_JOINT_GATEWAY_RESTART=PASS authenticated webhook dedupe; SIGKILL + byte-identical Reply replay accepted with Worker proof offline; no duplicate external send',flush=True)
        if args.faults:
            from faults import run_faults
            run_faults(h)
        if args.contracts:
            from session_scenarios import run_session
            from credentials_scenarios import run_credentials
            from capacity_scenarios import run_capacity
            run_session(h)
            run_capacity(h)
            h.stop_fault_workers()
            h.start_worker()
            run_credentials(h)
        return 0
    finally:h.close()

if __name__=='__main__':raise SystemExit(main())
