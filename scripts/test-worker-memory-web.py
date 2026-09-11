#!/usr/bin/env python3
"""Own one isolated stack for real browser Memory, Session, Artifact, Knowledge, MCP, Sequence, Parallel or Loop publication."""
import argparse
import copy
import json
import hashlib
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.request import urlopen, Request
from urllib.parse import quote, urlencode

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'worker-v1-joint'))
from memory_joint_fixture import MemoryHarness, MemoryModelFixture, TOOLS
import channel_lab_fixture as gateway_fixture
from faults import wait_success

class WebPublicationMixin:
    def api(self, method, path, body=None, status=200, idem=None):
        if method == 'POST' and path == '/v1/auth/login':
            self.login_user = body['username']
        if method == 'POST' and path == '/v1/me/change-password' and getattr(self, 'login_user', '') == 'joint-owner':
            self.owner_password = body['new_password']
        if method == 'POST' and path.endswith('/channel-bindings'):
            body = copy.deepcopy(body)
            body['target']['revision_number'] = self.revision_number
        return super().api(method, path, body, status, idem)

class WebHarness(WebPublicationMixin, MemoryHarness):
    pass

ARTIFACT_GUI_NAME = 'gui-upload.txt'
ARTIFACT_GUI_BYTES = b'artifact GUI accepted bytes\n'

def wait_browser_marker(path, web, timeout):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise RuntimeError('browser marker timeout: ' + path.name)
        if web.poll() is not None:
            raise RuntimeError('isolated Web exited during browser test')
        time.sleep(1)
    result = json.loads(path.read_text())
    assert result['result'] == 'PASS'
    return result

def verify_artifact_browser_download(marker, artifacts, run_id):
    assert marker['run_id'] == run_id and marker['name'] == ARTIFACT_GUI_NAME and marker['version'] == 0
    assert marker['size_bytes'] == len(ARTIFACT_GUI_BYTES)
    assert marker['sha256'] == hashlib.sha256(ARTIFACT_GUI_BYTES).hexdigest()
    path = Path(marker['download_file']).resolve()
    assert path.is_relative_to(artifacts.resolve()), 'browser download must be an owned evidence file'
    assert path.read_bytes() == ARTIFACT_GUI_BYTES, 'actual browser download bytes differ'
    return path

def run_artifact_browser_rounds(h, publication, marker, coordination, web, timeout, evidence, save):
    from artifact_joint_fixture import assert_artifact
    from faults import submit, run, head
    view = publication['manifest_view']
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 2
    def success(text, previous=None):
        offset = len(h.model.output_snapshot())
        run_id = submit(h, text, '42')
        delivery = h.wait_delivery(run_id)
        accepted = wait_success(h, run_id)
        actual = run(h, run_id)
        current_head = head(h, actual)
        route = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])['Route']
        assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
        assert current_head['accepted_ref'] == accepted['candidate']['candidate_ref'] and current_head['accepted_digest'] == accepted['candidate']['content_digest']
        assert accepted['candidate']['parent_ref'] == (previous['candidate']['candidate_ref'] if previous else '')
        assert accepted['candidate']['parent_digest'] == (previous['candidate']['content_digest'] if previous else '')
        assert delivery['final_text'] == 'artifact final: ' + text
        outputs = h.model.output_snapshot()[offset:]
        assert len(outputs) == 1 and outputs[0]['input'] == text
        return dict(input=text, run_id=run_id, run=actual, route=route, head=current_head, delivery=delivery, candidate=accepted['candidate'], completion=accepted['completion'], tool_results=outputs[0]['tool_results'])
    seed = success('artifact-v0')
    evidence['rounds'] = [seed]
    source = Path(h.write('gui-upload.txt', ARTIFACT_GUI_BYTES.decode()))
    ready = dict(result='READY', run_id=seed['run_id'], name=ARTIFACT_GUI_NAME, version=0, content=ARTIFACT_GUI_BYTES.decode(), upload_file=str(source), mime_type='text/plain', deployment_id=h.deployment_id, revision_number=h.revision_number, manifest_id=h.manifest_id)
    (coordination / 'artifact-ready.json').write_text(json.dumps(ready, indent=2) + '\n')
    evidence['gui_upload'] = 'PENDING';save()
    print('ARTIFACT_GUI_UPLOAD_READY=' + str(coordination / 'artifact-ready.json'), flush=True)
    uploaded = wait_browser_marker(coordination / 'gui-artifact.json', web, timeout)
    download = verify_artifact_browser_download(uploaded, h.artifacts, seed['run_id'])
    # Independent owner readback verifies the same immutable name/version, never substitutes for GUI upload.
    path = '/v1/tenants/' + quote(h.tenant_id, safe='') + '/deployments/' + quote(h.deployment_id, safe='') + '/revisions/' + str(h.revision_number) + '/artifacts/' + quote(ARTIFACT_GUI_NAME, safe='')
    request = Request(h.urls['control'] + path + '?' + urlencode({'run_id': seed['run_id'], 'version': 0}), method='GET')
    with h.opener.open(request, timeout=10) as response:
        raw, status = response.read(16 * 1024 * 1024 + 1), response.status
    assert status == 200 and raw == ARTIFACT_GUI_BYTES == download.read_bytes()
    h.gui_artifact_expected = dict(name=ARTIFACT_GUI_NAME, content=ARTIFACT_GUI_BYTES.decode(), version=0, mime_type='text/plain')
    follower = success('artifact-gui-read', seed)
    assert follower['run']['session_id'] == seed['run']['session_id']
    assert len(follower['tool_results']) == 1
    loaded = follower['tool_results'][0]
    assert_artifact(loaded, ARTIFACT_GUI_NAME, 0, raw, loaded=True, mime_type='text/plain')
    assert loaded['ref'] == uploaded['ref']
    metadata, objects = h.metadata_state(), h.object_state()
    files = [item for item in metadata['files'] if item['filename'] == ARTIFACT_GUI_NAME]
    assert len(files) == 1
    report = [item for item in metadata['files'] if item['filename'] == 'report.bin']
    assert len(report) == 1 and files[0]['scope_id'] == report[0]['scope_id']
    versions = [item for item in metadata['versions'] if item['file_id'] == files[0]['file_id']]
    assert len(versions) == 1 and versions[0]['version'] == 0
    content = next(item for item in objects if item['key'] == versions[0]['object_key'])
    assert content['bytes_hex'] == raw.hex() and content['sha256'] == uploaded['sha256']
    assert versions[0]['content_sha256'] == uploaded['sha256'] and versions[0]['content_length'] == len(raw)
    evidence.update(result='PASS', gui_upload='PASS', browser_upload=uploaded, worker_used_gui_manifest=True, rounds=[seed, follower], storage={'metadata': metadata, 'objects': objects}, next_run_loaded_gui_bytes=True, http_download_exact=True, model_requests=h.model.snapshot(), model_outputs=h.model.output_snapshot())


KNOWLEDGE_GUI_NAME = 'gui-reference.txt'
KNOWLEDGE_GUI_TEXT = 'GUI imported canary LILY-526.'

def verify_knowledge_browser_import(marker, publication):
    assert marker['result'] == 'PASS' and marker['http_status'] == 200
    assert marker['deployment_id'] == publication['deployment_id'] and marker['revision_number'] == publication['revision_number']
    assert marker['manifest_id'] == publication['manifest_id'] and marker['resource'] == 'docs'
    assert marker['name'] == KNOWLEDGE_GUI_NAME
    assert marker['text_sha256'] == hashlib.sha256(KNOWLEDGE_GUI_TEXT.encode()).hexdigest()
    assert type(marker['documents']) is int and marker['documents'] == 1

def run_knowledge_browser_round(h, publication, marker, coordination, web, timeout, evidence, save):
    from knowledge_joint_fixture import VECTOR_NAME, CALLABLE_NAME
    from faults import submit, run, head
    view = publication['manifest_view']
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 2
    h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(h.manifest_id)) == [[h.manifest_digest]], 'GUI Knowledge Manifest durable Worker projection')
    before = h.knowledge_state()
    assert before == []
    ready = dict(result='READY', name=KNOWLEDGE_GUI_NAME, text=KNOWLEDGE_GUI_TEXT, resource='docs', deployment_id=h.deployment_id, revision_number=h.revision_number, manifest_id=h.manifest_id)
    (coordination / 'knowledge-ready.json').write_text(json.dumps(ready, indent=2) + '\n')
    evidence['gui_import'] = 'PENDING'; save()
    print('KNOWLEDGE_GUI_IMPORT_READY=' + str(coordination / 'knowledge-ready.json'), flush=True)
    imported = wait_browser_marker(coordination / 'gui-knowledge.json', web, timeout)
    verify_knowledge_browser_import(imported, publication)
    after_import = h.knowledge_state()
    assert len(after_import) == 1
    point = after_import[0]
    assert point['payload']['document']['name'] == KNOWLEDGE_GUI_NAME and point['payload']['document']['content'] == KNOWLEDGE_GUI_TEXT
    assert point['vector'] == {VECTOR_NAME: [1, 0, 0]}
    imported_embeddings = h.model.embeddings()
    assert len(imported_embeddings) == 1 and KNOWLEDGE_GUI_TEXT in json.dumps(imported_embeddings[0])
    h.gui_knowledge_expected = dict(name=KNOWLEDGE_GUI_NAME, text=KNOWLEDGE_GUI_TEXT)
    run_id = submit(h, 'knowledge-gui-query', '42')
    delivery = h.wait_delivery(run_id)
    result = wait_success(h, run_id)
    actual = run(h, run_id); accepted = head(h, actual)
    route = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])['Route']
    assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
    assert (accepted['accepted_ref'], accepted['accepted_digest']) == (result['candidate']['candidate_ref'], result['candidate']['content_digest'])
    assert result['candidate']['parent_ref'] == ''
    calls, outputs, embeddings = h.model.snapshot(), h.model.output_snapshot(), h.model.embeddings()
    assert len(calls) == 2 and len(outputs) == 1 and len(embeddings) == 2
    assert all([item['function']['name'] for item in call['tools']] == [CALLABLE_NAME] for call in calls)
    assert all(call['model'] == h.model_name and call['max_completion_tokens'] == view['execution']['max_output_tokens'] for call in calls)
    documents = outputs[0]['tool_results'][0]['documents']
    assert len(documents) == 1 and documents[0]['text'] == KNOWLEDGE_GUI_TEXT and documents[0]['id'] == point['payload']['document']['id']
    assert delivery['final_text'] == 'knowledge final: ' + KNOWLEDGE_GUI_TEXT
    assert h.knowledge_state() == after_import, 'search must not mutate imported Knowledge'
    evidence.update(result='PASS', gui_import='PASS', browser_import=imported, worker_used_gui_manifest=True, run_id=run_id, run=actual, route=route, candidate=result['candidate'], completion=result['completion'], head=accepted, delivery=delivery, initial_knowledge=before, imported_knowledge=after_import, final_knowledge=h.knowledge_state(), model_requests=calls, model_outputs=outputs, embedding_requests=embeddings, imported_bytes_retrieved=True, external_embedding_verified=False)


MCP_GUI_CAPABILITY = 'mcp.search'

def verify_mcp_browser_publication(publication, marker, server_url):
    from mcp_joint_fixture import SELECTED
    view = publication['manifest_view']
    assert publication['deployment_id'] == marker['deployment_id']
    assert publication['revision_number'] == marker['revision_number'] == 2
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 2
    node = view['agent_plan']['nodes'][view['agent_plan']['root']]
    assert node['tool_resources'] == ['search'] and node['callable_entries'] == ['tools/search']
    assert set(view['resources']['tools']) == {'search'}, 'Manifest must contain only the selected MCP resource'
    assert marker['profile_credential_reopen'] == {'result': 'PASS', 'profile_id': marker['profile_id'], 'profile_revision_number': 2, 'action': 'keep', 'input_empty': True, 'original_token_absent': True}, 'browser must reopen the published Profile and assert write-only credentials'
    resource = view['resources']['tools']['search']
    assert resource['kind'] == 'mcp_streamable_http' and resource['capability'] == MCP_GUI_CAPABILITY
    assert (resource['server_url'], resource['toolset_name'], resource['tool_name']) == (server_url, 'joint_mcp', SELECTED)
    assert resource['auth'] == {'kind': 'bearer', 'credential_present': True}
    return resource

def verify_mcp_browser_exchange(calls, events, final_text, model_name, max_output_tokens):
    from mcp_joint_fixture import ANSWER, NORMAL, SELECTED, assert_declaration, tool_text
    assert len(calls) == 2
    values, call_ids = [], set()
    for call in calls:
        assert_declaration(call)
        assert call['model'] == model_name and call['max_completion_tokens'] == max_output_tokens and 'max_tokens' not in call
        messages = call['messages']
        start = max(i for i, message in enumerate(messages) if message.get('role') == 'user' and message.get('content') == NORMAL)
        for message in messages[start + 1:]:
            if message.get('role') == 'tool' and message['tool_call_id'] not in call_ids:
                call_ids.add(message['tool_call_id'])
                values.append(tool_text(message['content']))
    assert values == [ANSWER], 'SDK must consume exactly the real selected MCP result'
    executed = [event for event in events if event['method'] == 'tool_execution']
    assert len(executed) == 1
    assert executed[0]['authenticated'] and executed[0]['name'] == SELECTED and executed[0]['query'] == 'orchid' and not executed[0].get('is_error', False)
    assert final_text == ANSWER, 'Final must preserve the actual multiline MCP response'
    return values

def run_mcp_browser_round(h, publication, marker, evidence):
    from mcp_joint_fixture import NORMAL, SELECTED, UNSELECTED
    from faults import submit, run, head, rows
    verify_mcp_browser_publication(publication, marker, h.mcp_url)
    h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(h.manifest_id)) == [[h.manifest_digest]], 'GUI MCP Manifest durable Worker projection')
    state = h.mcp_state()
    assert state['registered_tools'] == [SELECTED, UNSELECTED]
    server_offset, model_offset = len(state['events']), len(h.model.snapshot())
    run_id = submit(h, NORMAL, '42')
    delivery = h.wait_delivery(run_id)
    result = wait_success(h, run_id)
    actual = run(h, run_id)
    accepted = head(h, actual)
    route = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])['Route']
    assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
    assert actual['attempts'] == 1
    assert (accepted['accepted_ref'], accepted['accepted_digest']) == (result['candidate']['candidate_ref'], result['candidate']['content_digest'])
    assert result['candidate']['parent_ref'] == '' and result['candidate']['parent_digest'] == ''
    calls, state = h.model.snapshot()[model_offset:], h.mcp_state()
    events = state['events'][server_offset:]
    values = verify_mcp_browser_exchange(calls, events, delivery['final_text'], h.model_name, publication['manifest_view']['execution']['max_output_tokens'])
    receipts = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(run_id))
    parts = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(run_id) + ' ORDER BY p.part_index')
    assert len(receipts) == 1 and receipts[0]['outcome'] == 'ACCEPTED'
    assert parts and all(part['state'] == 'ACCEPTED' for part in parts) and ''.join(part['body'] for part in parts) == delivery['final_text']
    evidence.update(result='PASS', worker_used_gui_manifest=True, mcp_server='ACTUAL_TRPC_MCP_GO_V0.0.10', im='REAL_CHANNEL_LAB', embedding='NOT_USED', run_id=run_id, run=actual, route=route, candidate=result['candidate'], completion=result['completion'], head=accepted, delivery=delivery, model_requests=calls, tool_results=values, mcp_events=events, mcp_state=state, gateway_receipts=receipts, gateway_parts=parts)


def verify_sequence_browser_publication(publication, marker, server_url):
    from mcp_joint_fixture import SELECTED
    view = publication['manifest_view']
    assert publication['deployment_id'] == marker['deployment_id'] and publication['revision_number'] == marker['revision_number'] == 2
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 2
    assert marker['profile_credential_reopen'] == {'result': 'PASS', 'profile_id': marker['profile_id'], 'profile_revision_number': 2, 'action': 'keep', 'input_empty': True, 'original_token_absent': True}
    plan = view['agent_plan']; nodes = plan['nodes']
    assert plan['root'] == 'workflow' and set(nodes) == {'workflow', 'prepare', 'researcher', 'writer'}
    assert nodes['workflow']['kind'] == nodes['prepare']['kind'] == 'sequence'
    assert nodes['workflow']['children'] == ['prepare', 'writer'] and nodes['prepare']['children'] == ['researcher']
    assert nodes['researcher']['kind'] == nodes['writer']['kind'] == 'llm'
    assert nodes['researcher']['model_resource'] == 'primary' and nodes['writer']['model_resource'] == 'writer'
    assert nodes['researcher']['tool_resources'] == ['search'] and nodes['researcher']['callable_entries'] == ['tools/search']
    assert not nodes['writer']['tool_resources'] and not nodes['writer']['callable_entries']
    assert set(view['resources']['models']) == {'primary', 'writer'} and set(view['resources']['tools']) == {'search'}
    resource = view['resources']['tools']['search']
    assert resource['kind'] == 'mcp_streamable_http' and resource['capability'] == MCP_GUI_CAPABILITY
    assert (resource['server_url'], resource['toolset_name'], resource['tool_name']) == (server_url, 'joint_mcp', SELECTED)
    assert resource['auth'] == {'kind': 'bearer', 'credential_present': True}



def verify_sequence_candidate_content(candidate, exchange, delivery):
    from sequence_joint_fixture import all_strings
    content = set(all_strings(candidate['content']))
    assert exchange['first_output'] in content and exchange['terminal_output'] in content
    assert delivery['final_text'] == exchange['terminal_output'] and delivery['final_text'] != exchange['first_output']

def run_sequence_browser_round(h, publication, marker, evidence):
    from sequence_joint_fixture import NORMAL, require_exchange, require_accepted_history
    from faults import submit, run, head, rows
    verify_sequence_browser_publication(publication, marker, h.mcp_url)
    h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(h.manifest_id)) == [[h.manifest_digest]], 'GUI Sequence Manifest durable Worker projection')
    envelope = rows(h, "SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0]['manifest']
    models = envelope['content']['resources']['models']
    assert models['primary']['credential']['credential_id'] != models['writer']['credential']['credential_id']
    state = h.mcp_state(); assert state['registered_tools'] == ['selected_search', 'unselected_secret']
    model_offset, server_offset = len(h.model.snapshot()), len(state['events'])
    run_id = submit(h, NORMAL, '42')
    delivery = h.wait_delivery(run_id)
    result = wait_success(h, run_id)
    actual, candidate = run(h, run_id), result['candidate']; accepted = head(h, actual)
    route = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(run_id))[0]['route']
    assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
    assert actual['attempts'] == actual['session_sequence'] == 1
    assert (accepted['accepted_ref'], accepted['accepted_digest']) == (candidate['candidate_ref'], candidate['content_digest'])
    h.wait(lambda: len(h.model.snapshot()[model_offset:]) == 3 and all(c.get('complete') for c in h.model.snapshot()[model_offset:]), 'Sequence model observation completion')
    calls = h.model.snapshot()[model_offset:]
    exchange = require_exchange(calls, NORMAL, live=False, limit=publication['manifest_view']['execution']['max_output_tokens'])
    require_accepted_history(None, candidate, [call['request'] for call in calls])
    verify_sequence_candidate_content(candidate, exchange, delivery)
    state = h.mcp_state(); events = state['events'][server_offset:]
    executed = [event for event in events if event['method'] == 'tool_execution']
    assert len(executed) == 1 and executed[0]['name'] == 'selected_search' and executed[0]['authenticated'] and executed[0]['query'] == 'orchid'
    receipts = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(run_id))
    parts = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(run_id) + ' ORDER BY p.part_index')
    assert len(receipts) == 1 and receipts[0]['outcome'] == 'ACCEPTED'
    assert parts and all(part['state'] == 'ACCEPTED' for part in parts) and ''.join(part['body'] for part in parts) == delivery['final_text']
    evidence.update(result='PASS', worker_used_gui_manifest=True, mcp_server='ACTUAL_TRPC_MCP_GO_V0.0.10', im='REAL_CHANNEL_LAB', embedding='NOT_USED', run_id=run_id, run=actual, route=route, candidate=candidate, completion=result['completion'], head=accepted, delivery=delivery, exchange=exchange, model_requests=calls, mcp_events=events, mcp_state=state, gateway_receipts=receipts, gateway_parts=parts, distinct_model_credential_ids=True)

PARALLEL_GUI_INSTRUCTION_SUFFIX = ' Use both actual branch outputs in the declared order.'


def verify_parallel_browser_publication(publication, marker, server_url):
    from parallel_joint_fixture import BRANCHES, ROLES, SLOTS, TOOLS, MODELS, SELECTED
    view = publication['manifest_view']
    assert publication['deployment_id'] == marker['deployment_id'] and publication['revision_number'] == marker['revision_number'] == 2
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 2
    assert marker['profile_credential_reopen'] == {'result': 'PASS', 'profile_id': marker['profile_id'], 'profile_revision_number': 2, 'action': 'keep', 'resources': ['search_a', 'search_b'], 'input_empty': True, 'original_token_absent': True}
    plan = view['agent_plan']; nodes = plan['nodes']
    assert plan['root'] == 'workflow' and set(nodes) == {'workflow', 'research', *ROLES}
    assert nodes['workflow']['kind'] == 'sequence' and nodes['workflow']['children'] == ['research', 'aggregator']
    assert nodes['research']['kind'] == 'parallel' and nodes['research']['children'] == list(BRANCHES)
    assert nodes['aggregator']['instruction'].endswith(PARALLEL_GUI_INSTRUCTION_SUFFIX)
    assert set(view['resources']['models']) == set(SLOTS.values()) and set(view['resources']['tools']) == set(TOOLS.values())
    for role in ROLES:
        node = nodes[role]
        assert node['kind'] == 'llm' and node['model_resource'] == SLOTS[role]
        assert view['resources']['models'][SLOTS[role]]['model'] == MODELS[role]
        assert node['tool_resources'] == ([TOOLS[role]] if role in BRANCHES else [])
        assert node['callable_entries'] == (['tools/' + TOOLS[role]] if role in BRANCHES else [])
        assert not node.get('knowledge_resources') and not node.get('memory') and not node.get('artifact')
    for resource in view['resources']['tools'].values():
        assert resource['kind'] == 'mcp_streamable_http' and resource['capability'] == MCP_GUI_CAPABILITY
        assert (resource['server_url'], resource['toolset_name'], resource['tool_name']) == (server_url, 'joint_mcp', SELECTED)
        assert resource['auth'] == {'kind': 'bearer', 'credential_present': True}


def verify_parallel_candidate_content(candidate, exchange, delivery):
    from parallel_joint_fixture import BRANCHES, all_strings
    assert set(exchange['branch_outputs']) == set(BRANCHES)
    content = set(all_strings(candidate['content']))
    assert all(exchange['branch_outputs'][role] in content for role in BRANCHES)
    assert exchange['terminal_output'] in content
    assert delivery['final_text'] == exchange['terminal_output']
    assert delivery['final_text'] not in exchange['branch_outputs'].values()


def run_parallel_browser_round(h, publication, marker, evidence):
    from parallel_joint_fixture import A_FIRST, require_exchange, require_accepted_history
    from faults import submit, run, head, rows
    verify_parallel_browser_publication(publication, marker, h.mcp_url)
    h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(h.manifest_id)) == [[h.manifest_digest]], 'GUI Parallel Manifest durable Worker projection')
    envelope = rows(h, "SELECT convert_from(envelope,'UTF8')::json AS manifest FROM worker.runtime_manifests WHERE manifest_id=" + h.quote(h.manifest_id))[0]['manifest']
    resources = envelope['content']['resources']
    model_ids = {resource['credential']['credential_id'] for resource in resources['models'].values()}
    tool_ids = {resource['auth']['credential']['credential_id'] for resource in resources['tools'].values()}
    assert len(model_ids) == 3 and len(tool_ids) == 2 and not model_ids.intersection(tool_ids)
    state = h.mcp_state(); assert state['registered_tools'] == ['selected_search', 'unselected_secret']
    model_offset, server_offset = len(h.model.snapshot()), len(state['events'])
    run_id = submit(h, A_FIRST, '42')
    delivery = h.wait_delivery(run_id)
    result = wait_success(h, run_id)
    actual, candidate = run(h, run_id), result['candidate']; accepted = head(h, actual)
    route = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(run_id))[0]['route']
    assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
    assert actual['attempts'] == actual['session_sequence'] == 1
    assert (accepted['accepted_ref'], accepted['accepted_digest']) == (candidate['candidate_ref'], candidate['content_digest'])
    h.wait(lambda: len(h.model.snapshot()[model_offset:]) == 5 and all(c.get('complete') for c in h.model.snapshot()[model_offset:]), 'Parallel model observation completion')
    calls = h.model.snapshot()[model_offset:]
    exchange = require_exchange(calls, A_FIRST, live=False, limit=publication['manifest_view']['execution']['max_output_tokens'])
    require_accepted_history(None, candidate, [call['request'] for call in calls])
    verify_parallel_candidate_content(candidate, exchange, delivery)
    state = h.mcp_state(); events = state['events'][server_offset:]
    executed = [event for event in events if event['method'] == 'tool_execution']
    assert len(executed) == 2 and all(event['name'] == 'selected_search' and event['authenticated'] and event['query'] == 'orchid' for event in executed)
    receipts = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(run_id))
    parts = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(run_id) + ' ORDER BY p.part_index')
    assert len(receipts) == 1 and receipts[0]['outcome'] == 'ACCEPTED' and receipts[0]['reason'] == ''
    assert receipts[0]['intent_id'] == delivery['intent_id'] and receipts[0]['run_id'] == run_id
    assert parts and all(part['state'] == 'ACCEPTED' for part in parts) and ''.join(part['body'] for part in parts) == delivery['final_text']
    assert len(delivery['outgoing_added']) == len(parts) and ''.join(message['text'] for message in delivery['outgoing_added']) == delivery['final_text']
    evidence.update(result='PASS', worker_used_gui_manifest=True, mcp_server='ACTUAL_TRPC_MCP_GO_V0.0.10', im='REAL_CHANNEL_LAB', embedding='NOT_USED', run_id=run_id, run=actual, route=route, candidate=candidate, completion=result['completion'], head=accepted, delivery=delivery, exchange=exchange, model_requests=calls, mcp_events=events, mcp_state=state, gateway_receipts=receipts, gateway_parts=parts, distinct_model_credential_ids=True, distinct_tool_credential_ids=True)

LOOP_GUI_INSTRUCTION_SUFFIX = ' Use the actual prior iteration output without skipping the declared iteration.'


def verify_loop_browser_publication(publication, marker):
    from loop_joint_fixture import MODEL
    view = publication['manifest_view']
    assert publication['deployment_id'] == marker['deployment_id'] and publication['revision_number'] == marker['revision_number'] == 2
    assert view['sources']['agent']['agent_id'] == marker['agent_id'] and view['sources']['agent']['version_number'] == marker['agent_version_number'] == 2
    # Loop changes Agent structure only. Keep the explicitly selected existing
    # Profile revision rather than manufacturing an unrelated credential update.
    assert view['sources']['profile']['profile_id'] == marker['profile_id'] and view['sources']['profile']['revision_number'] == marker['profile_revision_number'] == 1
    assert marker['initial_max_iterations'] == 1 and marker['published_max_iterations'] == 2
    assert marker['profile_credential_reopen'] == {'result': 'PASS', 'profile_id': marker['profile_id'], 'profile_revision_number': 1, 'resource': 'primary', 'action': 'keep', 'input_empty': True, 'original_key_absent': True}
    plan = view['agent_plan']; nodes = plan['nodes']
    assert plan['root'] == 'workflow' and set(nodes) == {'workflow', 'assistant'}
    assert nodes['workflow'] == {'kind': 'loop', 'body': 'assistant', 'max_iterations': 2}
    node = nodes['assistant']
    assert node['kind'] == 'llm' and node['model_resource'] == 'primary'
    assert node['instruction'].endswith(LOOP_GUI_INSTRUCTION_SUFFIX)
    assert node['tool_resources'] == [] and node['callable_entries'] == [] and node['knowledge_resources'] == []
    assert not node.get('memory') and not node.get('artifact') and not node.get('add_session_summary')
    assert set(view['resources']['models']) == {'primary'} and not view['resources']['tools'] and not view['resources']['knowledge']
    assert view['resources']['models']['primary']['model'] == MODEL


def run_loop_browser_round(h, publication, marker, evidence):
    from loop_joint_fixture import NORMAL, require_exchange, require_accepted_history
    from faults import submit, run, head, rows
    verify_loop_browser_publication(publication, marker)
    h.wait(lambda: h.sql('SELECT content_digest FROM worker.runtime_manifests WHERE manifest_id=' + h.quote(h.manifest_id)) == [[h.manifest_digest]], 'GUI Loop Manifest durable Worker projection')
    assert not hasattr(h, 'mcp_url'), 'no MCP server is part of this GUI scenario'
    model_offset = len(h.model.snapshot())
    run_id = submit(h, NORMAL, '42')
    delivery = h.wait_delivery(run_id)
    result = wait_success(h, run_id)
    actual, candidate = run(h, run_id), result['candidate']; accepted = head(h, actual)
    route = rows(h, "SELECT request_json->'Route' AS route FROM worker.execution_runs WHERE run_id=" + h.quote(run_id))[0]['route']
    assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
    assert actual['attempts'] == actual['session_sequence'] == 1
    assert (accepted['accepted_ref'], accepted['accepted_digest']) == (candidate['candidate_ref'], candidate['content_digest'])
    h.wait(lambda: len(h.model.snapshot()[model_offset:]) == 2 and all(c.get('complete') for c in h.model.snapshot()[model_offset:]), 'Loop model observation completion')
    calls = h.model.snapshot()[model_offset:]
    exchange = require_exchange(calls, NORMAL, live=False, limit=publication['manifest_view']['execution']['max_output_tokens'])
    require_accepted_history(None, candidate, [call['request'] for call in calls])
    verify_sequence_candidate_content(candidate, exchange, delivery)
    receipts = rows(h, 'SELECT stream_id,stream_sequence,outcome,reason,intent_id,run_id FROM gateway.gateway_reply_transport_receipts WHERE run_id=' + h.quote(run_id))
    parts = rows(h, 'SELECT p.part_id,p.body,p.state FROM gateway.gateway_delivery_parts p JOIN gateway.gateway_delivery_intents i USING(intent_id) WHERE i.run_id=' + h.quote(run_id) + ' ORDER BY p.part_index')
    assert len(receipts) == 1 and receipts[0]['outcome'] == 'ACCEPTED' and receipts[0]['reason'] == ''
    assert receipts[0]['intent_id'] == delivery['intent_id'] and receipts[0]['run_id'] == run_id
    assert parts and all(part['state'] == 'ACCEPTED' for part in parts) and ''.join(part['body'] for part in parts) == delivery['final_text']
    assert len(delivery['outgoing_added']) == len(parts) and ''.join(message['text'] for message in delivery['outgoing_added']) == delivery['final_text']
    evidence.update(result='PASS', worker_used_gui_manifest=True, mcp_server='NOT_STARTED', im='REAL_CHANNEL_LAB', embedding='NOT_USED', run_id=run_id, run=actual, route=route, candidate=candidate, completion=result['completion'], head=accepted, delivery=delivery, exchange=exchange, model_requests=calls, gateway_receipts=receipts, gateway_parts=parts)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--coordination', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=1800)
    parser.add_argument('--backend', choices=('postgresql', 'redis'), default='postgresql')
    parser.add_argument('--scenario', choices=('memory', 'session', 'artifact', 'knowledge', 'mcp', 'sequence', 'parallel', 'loop'), default='memory')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    harness = WebHarness
    model_fixture = MemoryModelFixture
    if args.scenario == 'loop':
        from loop_joint_fixture import LoopHarness, MODEL
        class LoopWebHarness(WebPublicationMixin, LoopHarness):
            pass
        harness = LoopWebHarness
    if args.scenario == 'parallel':
        from parallel_joint_fixture import ParallelHarness, MODELS
        class ParallelWebHarness(WebPublicationMixin, ParallelHarness):
            pass
        harness = ParallelWebHarness
    if args.scenario == 'sequence':
        from sequence_joint_fixture import SequenceHarness, RESEARCH_MODEL
        class SequenceWebHarness(WebPublicationMixin, SequenceHarness):
            pass
        harness = SequenceWebHarness
    if args.scenario == 'mcp':
        from mcp_joint_fixture import MCPHarness
        class MCPWebHarness(WebPublicationMixin, MCPHarness):
            pass
        harness = MCPWebHarness
    if args.scenario == 'knowledge':
        from knowledge_joint_fixture import KnowledgeHarness, KnowledgeModelFixture
        class KnowledgeWebHarness(WebPublicationMixin, KnowledgeHarness):
            pass
        harness, model_fixture = KnowledgeWebHarness, KnowledgeModelFixture
    if args.scenario == 'artifact':
        from artifact_joint_fixture import ArtifactHarness, ArtifactModelFixture
        class ArtifactWebHarness(WebPublicationMixin, ArtifactHarness):
            pass
        harness, model_fixture = ArtifactWebHarness, ArtifactModelFixture
    if args.scenario == 'session':
        if args.backend != 'redis':
            parser.error('session scenario requires --backend redis')
        from redis_session_joint_fixture import RedisSessionHarness, SummaryModelFixture, wait_redis_success
        class RedisSessionWebHarness(WebPublicationMixin, RedisSessionHarness):
            pass
        harness = RedisSessionWebHarness
        model_fixture = SummaryModelFixture
    if args.backend == 'redis' and args.scenario == 'memory':
        from redis_memory_joint_fixture import RedisMemoryHarness
        class RedisWebHarness(WebPublicationMixin, RedisMemoryHarness):
            pass
        harness = RedisWebHarness
    h = harness(root, args.artifacts, **({'model_name': MODEL} if args.scenario == 'loop' else {'model_name': MODELS['research_a']} if args.scenario == 'parallel' else {'model_name': RESEARCH_MODEL} if args.scenario == 'sequence' else {'model_name': 'joint-mcp-fixture'} if args.scenario == 'mcp' else {}))
    if args.scenario == 'loop': h.initial_max_iterations = 1
    backend_id = None if args.scenario in ('mcp', 'sequence', 'parallel', 'loop') else 'joint-knowledge-qdrant' if args.scenario == 'knowledge' else 'joint-artifact-s3' if args.scenario == 'artifact' else ('joint-session-redis' if args.scenario == 'session' else ('joint-memory-redis' if args.backend == 'redis' else 'joint-memory-pg'))
    web = None
    web_log = None
    evidence = {'result': 'PENDING', 'gui': 'PENDING', 'external_model': 'DETERMINISTIC_HTTP_FIXTURE', 'shared_services_changed': False}
    target = h.artifacts / (args.scenario + '-web.json')
    def save():
        target.write_text(h.redact(json.dumps(evidence, indent=2, ensure_ascii=False)) + '\n')
    try:
        h.provision()
        if args.scenario == 'loop':
            h.prepare_loop()
        elif args.scenario == 'parallel':
            h.prepare_parallel()
        elif args.scenario == 'sequence':
            h.prepare_sequence()
        elif args.scenario == 'mcp':
            h.prepare_mcp()
        else:
            h.model.close()
            h.model = model_fixture(h)
            h.urls['model'] = h.model.url
        h.control_start(gateway_fixture.prepare(h))
        h.seed()
        h.start_worker()
        h.verify_dependencies()
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        web_url = 'http://127.0.0.1:' + str(port)
        web_log = open(h.artifacts / 'web.log', 'w')
        # A dedicated process group is owned here, not the shared Docker Web.
        web = subprocess.Popen(['node', str(root / 'web/node_modules/next/dist/bin/next'), 'dev', '--hostname', '127.0.0.1', '--port', str(port)], cwd=root / 'web', env=dict(os.environ, CONTROL_API_BASE=h.urls['control'], NEXT_TELEMETRY_DISABLED='1'), stdout=web_log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 120
        while True:
            if web.poll() is not None:
                raise RuntimeError('isolated Web exited before readiness')
            try:
                with urlopen(web_url + '/login', timeout=2) as response:
                    if response.status == 200:
                        break
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError('isolated Web readiness timeout') from None
                time.sleep(.5)
        access = Path(h.work) / 'gui-access.json'
        secrets = {} if args.scenario in ('knowledge', 'mcp', 'sequence', 'parallel', 'loop') else {'artifact_access_key': h.artifact_access_key, 'artifact_secret_key': h.artifact_secret_key} if args.scenario == 'artifact' else {args.scenario + '_password': h.session_password if args.scenario == 'session' else h.memory_password}
        if args.scenario == 'loop': secrets = {'model_api_key': h.model.key, 'initial_max_iterations': h.initial_max_iterations}
        if args.scenario == 'knowledge': secrets = {'qdrant_api_key': h.qdrant_key, 'embedding_api_key': h.model.embedding_key}
        if args.scenario in ('mcp', 'sequence', 'parallel'):
            from mcp_joint_fixture import SELECTED
            secrets = {'mcp_bearer_token': h.mcp_token, 'mcp_server_url': h.mcp_url, 'mcp_tool_name': SELECTED, 'mcp_toolset_name': 'joint_mcp', 'mcp_capability': MCP_GUI_CAPABILITY}
        access.write_text(json.dumps({'web_url': web_url, 'username': 'joint-owner', 'password': h.owner_password, **secrets, 'tenant_id': h.tenant_id, 'agent_id': h.agent_id, 'profile_id': h.profile_id, 'deployment_id': h.deployment_id, 'backend_id': backend_id, 'backend_revision': 1, 'artifacts': str(h.artifacts)}))
        access.chmod(0o600)
        args.coordination.mkdir(parents=True, exist_ok=True)
        (args.coordination / 'ready.json').write_text(json.dumps({'url': web_url, 'private_access_file': str(access), 'artifacts': str(h.artifacts)}))
        print('MEMORY_WEB_READY=' + str(args.coordination / 'ready.json'), flush=True)
        done = args.coordination / 'gui-published.json'
        deadline = time.monotonic() + args.timeout
        while not done.exists():
            if time.monotonic() > deadline:
                raise RuntimeError('browser publication marker timeout')
            if web.poll() is not None:
                raise RuntimeError('isolated Web exited during browser test')
            time.sleep(1)
        marker = json.loads(done.read_text())
        assert marker['result'] == 'PASS'
        h.deployment_id, h.revision_number = marker['deployment_id'], marker['revision_number']
        publication = h.api('GET', '/v1/tenants/' + h.tenant_id + '/deployments/' + h.deployment_id + '/revisions/' + str(h.revision_number))
        view = publication['manifest_view']
        node = view['agent_plan']['nodes'][view['agent_plan']['root']]
        if args.scenario == 'memory':
            assert sorted(node['memory']['tools']) == sorted(TOOLS)
            selected_storage = node['memory']['resource']
        elif args.scenario == 'session':
            assert view['runtime']['summary']['enabled'] and node['add_session_summary']
            selected_storage = view['storage_roles']['session']
        elif args.scenario == 'loop':
            verify_loop_browser_publication(publication, marker)
        elif args.scenario == 'parallel':
            verify_parallel_browser_publication(publication, marker, h.mcp_url)
        elif args.scenario == 'sequence':
            verify_sequence_browser_publication(publication, marker, h.mcp_url)
        elif args.scenario == 'mcp':
            verify_mcp_browser_publication(publication, marker, h.mcp_url)
        elif args.scenario == 'knowledge':
            assert node['knowledge_resources'] == ['docs']
            assert view['resources']['knowledge']['docs']['backend']['backend_id'] == backend_id
        else:
            assert node['artifact'] == {'enabled': True, 'resource': 'artifact'}
            selected_storage = view['storage_roles']['artifact']
        if args.scenario not in ('knowledge', 'mcp', 'sequence', 'parallel', 'loop'):
            assert view['resources']['storage'][selected_storage]['backend']['backend_id'] == backend_id
        h.revision_id = publication['id']
        h.manifest_id, h.manifest_digest = publication['manifest_id'], publication['manifest_digest']
        evidence.update(gui='PASS', publication=publication, browser=marker)
        h.gateway = gateway_fixture.start(h)
        if args.scenario == 'memory':
            run_id = h.send_text('memory-six')
            delivery = h.wait_delivery(run_id)
            result = wait_success(h, run_id)
            assert delivery['final_text'] == 'memory final: memory-six'
            actual_request = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])
            assert actual_request['Route']['ManifestRef'] == h.manifest_id and actual_request['Route']['ManifestDigest'] == h.manifest_digest and actual_request['Route']['DeploymentRevisionID'] == h.revision_id
            evidence['worker_request_route'] = actual_request['Route']
            state = h.memory_state()
            assert len(state) == 1 and 'persistent orchid memory' in json.dumps(state)
            assert h.sql('SELECT memory_status FROM worker.execution_completions WHERE run_id=' + h.quote(run_id)) == [['APPLIED']]
            evidence.update(result='PASS', worker_used_gui_manifest=True, run_id=run_id, delivery=delivery, memory=state, completion=result['completion'])
        elif args.scenario == 'loop':
            run_loop_browser_round(h, publication, marker, evidence)
        elif args.scenario == 'parallel':
            run_parallel_browser_round(h, publication, marker, evidence)
        elif args.scenario == 'sequence':
            run_sequence_browser_round(h, publication, marker, evidence)
        elif args.scenario == 'mcp':
            run_mcp_browser_round(h, publication, marker, evidence)
        elif args.scenario == 'knowledge':
            run_knowledge_browser_round(h, publication, marker, args.coordination, web, args.timeout, evidence, save)
        elif args.scenario == 'artifact':
            run_artifact_browser_rounds(h, publication, marker, args.coordination, web, args.timeout, evidence, save)
        else:
            from faults import run, head
            rounds = []
            prior_summary = None
            prior_candidate = None
            for text in ('gui redis history seed', 'gui redis summary generate', 'gui redis summary consume'):
                offset = len(h.model.snapshot())
                run_id = h.send_text(text)
                delivery = h.wait_delivery(run_id)
                result = wait_redis_success(h, run_id)
                actual_request = json.loads(h.sql('SELECT request_json::text FROM worker.execution_runs WHERE run_id=' + h.quote(run_id))[0][0])
                route = actual_request['Route']
                assert (route['ManifestRef'], route['ManifestDigest'], route['DeploymentRevisionID']) == (h.manifest_id, h.manifest_digest, h.revision_id)
                candidate = result['candidate']
                accepted = head(h, run(h, run_id))
                assert (accepted['accepted_ref'], accepted['accepted_digest']) == (candidate['candidate_ref'], candidate['content_digest'])
                assert candidate['parent_ref'] == (prior_candidate['candidate_ref'] if prior_candidate else '')
                calls = h.model.snapshot()[offset:]
                primary = [call for call in calls if call['model'] == 'joint-fixture']
                assert len(primary) == 1
                if prior_summary:
                    assert prior_summary in json.dumps(primary[0]['messages'])
                summaries = candidate['content']['snapshot']['session'].get('summaries', {})
                if summaries:
                    prior_summary = next(iter(summaries.values()))['summary']
                    assert prior_summary == h.model.summary_outputs()[-1]
                assert delivery['final_text'] == 'joint answer: ' + text
                rounds.append({'run_id': run_id, 'route': route, 'candidate': candidate, 'head': accepted, 'delivery': delivery, 'calls': calls})
                prior_candidate = candidate
            assert prior_summary and len(h.model.summary_outputs()) >= 2
            assert len({item['candidate']['content']['identity']['session_id'] for item in rounds}) == 1
            evidence.update(result='PASS', worker_used_gui_manifest=True, rounds=rounds, session=h.session_state(), same_redis_summary=True, next_run_consumed_summary=True)
        save()
    except BaseException as exc:
        evidence.update(result='FAIL', error=h.redact(str(exc)))
        save()
        raise RuntimeError(evidence['error']) from None
    finally:
        if web is not None and web.poll() is None:
            os.killpg(web.pid, signal.SIGTERM)
            try:
                web.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(web.pid, signal.SIGKILL)
                web.wait(timeout=5)
        if web_log:
            web_log.close()
        evidence['web_exit'] = web.returncode if web is not None else None
        try:
            h.close()
            evidence['cleanup'] = 'PASS'
        except BaseException as exc:
            evidence.update(result='FAIL', cleanup_error=h.redact(str(exc)))
            raise
        finally:
            save()
            leaked = [str(path) for path in h.artifacts.rglob('*') if path.is_file() and any(secret.encode() in path.read_bytes() for secret in h.secrets if secret)]
            evidence['credential_leak_scan'] = {'result': 'FAIL' if leaked else 'PASS', 'files': leaked}
            if leaked:
                evidence['result'] = 'FAIL'
            save()
            if leaked:
                raise RuntimeError('credential leak in browser artifacts')
    print('WORKER_' + args.scenario.upper() + '_WEB=PASS', flush=True)

if __name__ == '__main__':
    main()
