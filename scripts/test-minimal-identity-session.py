#!/usr/bin/env python3
"""Real Control/Gateway/Worker/PG/NATS; explicit model and Telegram HTTP fixtures."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from harness import Harness
import gateway_fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', type=Path, required=True)
    args = parser.parse_args()
    h = Harness(Path(__file__).resolve().parents[1], args.artifacts)
    try:
        h.provision()
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.gateway = gateway_fixture.start(h)
        runs = []
        for text, sender in [('alice-first', 100), ('bob-first', 101)]:
            run = h.gateway.send_text(text, str(sender), sender_id=sender)
            h.wait_delivery(run)
            runs.append(run)
        h.stop_worker()
        h.start_worker(worker_id='worker-two')
        for text, sender in [('alice-next', 100), ('bob-next', 101)]:
            run = h.gateway.send_text(text, str(sender), sender_id=sender)
            h.wait_delivery(run)
            runs.append(run)
        assert len(h.model.requests) == 4, 'unexpected model call count'
        alice, bob = [json.dumps(h.model.requests[n]['messages']) for n in (2, 3)]
        assert 'alice-first' in alice and 'joint answer: alice-first' in alice and 'bob-first' not in alice
        assert 'bob-first' in bob and 'joint answer: bob-first' in bob and 'alice-first' not in bob
        rows = h.sql("SELECT r.run_id,r.session_id,r.session_sequence,r.status,i.identity_id FROM worker.execution_runs r JOIN worker.execution_session_identities i USING(tenant_id,session_id) ORDER BY r.accepted_at")
        assert len(rows) == 4 and all(r[3] == 'SUCCEEDED' for r in rows)
        assert rows[0][1] == rows[2][1] and rows[1][1] == rows[3][1] and rows[0][1] != rows[1][1]
        assert [r[2] for r in rows] == ['1', '1', '2', '2']
        identities = h.sql("SELECT provider,external_user_id,count(*) FROM worker.execution_social_identities GROUP BY provider,external_user_id ORDER BY external_user_id")
        assert identities == [['telegram', '100', '1'], ['telegram', '101', '1']]
        replay = h.gateway.verify_webhook_auth_and_replay(runs[0])
        time.sleep(1)
        assert len(h.model.requests) == 4
        assert h.sql("SELECT count(*) FROM worker.execution_runs") == [['4']]
        evidence = dict(result='PASS', business='two users each continue two rounds across Worker replacement', runs=rows, identities=identities, model_requests=h.model.requests, replay=replay, external_fixtures=['model HTTP/SSE', 'Telegram Bot API'])
        target = h.artifacts / 'minimal-identity-session.json'
        target.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
        assert json.loads(target.read_text()) == evidence
        print('MINIMAL_IDENTITY_SESSION_JOINT=PASS identities=2 sessions=2 rounds=4 worker_replacement=YES second_round_history=VERIFIED cross_user_history=ABSENT replay_model_calls=0 formal_finals=4')
    finally:
        h.close()


if __name__ == '__main__':
    main()
