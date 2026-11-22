"""追溯接口的端到端测试：用 SQLite 内存库替换 MySQL，不依赖外部服务。"""

import hashlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.models import tables
from src.services import workflow as workflow_service
from src.services.domain import SPEC
from src.services.security import hash_password
from src.utils.encoding import canonical
from src.utils.time import utc_time


class TraceApiTests(unittest.TestCase):
    def setUp(self):
        # StaticPool 让全部连接共享同一个内存库
        self.engine = create_engine(
            'sqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
        )
        tables.schema.create_all(self.engine)
        # 应用代码全部从模块属性取 engine，统一替换为内存库
        self.patches = [
            patch('src.config.settings.engine', self.engine),
            patch('src.services.workflow.engine', self.engine),
            patch('src.services.security.engine', self.engine),
        ]
        for item in self.patches:
            item.start()

        from src.main import app
        # 跳过 lifespan 里的 MySQL 依赖与演示种子
        self.client = TestClient(app)
        with self.engine.begin() as conn:
            conn.execute(tables.users.insert().values(
                username='reviewer', password_hash=hash_password('pw'), role='reviewer'))
            case_id = conn.execute(tables.cases.insert().values(
                slug='case-1', title='样例', payload=SPEC['sample'],
                author='operator', created_at=utc_time())).inserted_primary_key[0]
            run_id = conn.execute(tables.runs.insert().values(
                case_id=case_id, status='complete', owner='operator',
                created_at=utc_time(), finished_at=utc_time())).inserted_primary_key[0]
            report = {'summary': '封存报告', 'metrics': {}, 'sections': []}
            self.digest = hashlib.sha256(canonical(report)).hexdigest()
            self.evidence_id = conn.execute(tables.evidence.insert().values(
                run_id=run_id, report=report, digest=self.digest)).inserted_primary_key[0]

        token = self.client.post('/login', json={'username': 'reviewer', 'password': 'pw'}).json()['token']
        self.headers = {'Authorization': f'Bearer {token}'}

    def tearDown(self):
        for item in self.patches:
            item.stop()
        self.engine.dispose()

    def _get(self, query):
        return self.client.get(f'/evidence/{self.evidence_id}/trace?{query}', headers=self.headers)

    def test_requires_authentication(self):
        response = self.client.get(f'/evidence/{self.evidence_id}/trace?sign_id=N-07')
        self.assertEqual(response.status_code, 401)

    def test_trace_listed_sign(self):
        response = self._get('sign_id=N-07')
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body['status'], 'listed')
        self.assertEqual(body['stream'], 'NORTH')
        self.assertEqual(body['as_of']['content'], '桂花米酿｜18元')
        self.assertEqual(body['as_of']['source_sequence'], 14)
        self.assertEqual(body['digest'], self.digest)
        self.assertEqual(len(body['timeline']), 3)

    def test_trace_withdrawn_sign(self):
        response = self._get('sign_id=N-08')
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body['status'], 'withdrawn')
        self.assertIsNone(body['as_of'])

    def test_trace_unlisted_unknown_sign(self):
        response = self._get('sign_id=NO-SUCH')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['status'], 'unlisted')

    def test_trace_with_batch_filter(self):
        response = self._get('sign_id=N-07&batch=1')
        body = response.json()
        self.assertEqual(len(body['timeline']), 1)
        self.assertEqual(body['queried_batch'], 1)
        self.assertEqual(body['as_of']['content'], '桂花米酿｜22元')

    def test_trace_bad_batch_returns_422(self):
        response = self._get('sign_id=N-07&batch=99')
        self.assertEqual(response.status_code, 422)

    def test_trace_bad_identifier_returns_422(self):
        response = self._get('sign_id=bad/sign')
        self.assertEqual(response.status_code, 422)

    def test_unknown_evidence_returns_404(self):
        response = self.client.get('/evidence/999/trace?sign_id=N-07', headers=self.headers)
        self.assertEqual(response.status_code, 404)

    def test_trace_does_not_mutate_sealed_report(self):
        before = workflow_service.found(self.engine.connect(), tables.evidence, self.evidence_id)
        self._get('sign_id=N-07')
        self._get('sign_id=N-08&batch=2')
        after = workflow_service.found(self.engine.connect(), tables.evidence, self.evidence_id)
        self.assertEqual(before['report'], after['report'])
        self.assertEqual(before['digest'], after['digest'])
        self.assertIsNone(after['reviewer'])
        # 不产生审计写入
        with self.engine.connect() as conn:
            count = conn.execute(tables.audit.select()).fetchall()
        self.assertEqual(count, [])


if __name__ == '__main__':
    unittest.main()
