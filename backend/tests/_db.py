"""测试共用的内存数据库。

所有测试模块通过这里的 engine 访问同一个 SQLite 内存库，避免各模块自建
引擎导致「表建在 A 库、请求落到 B 库」的不一致。
"""
import os

os.environ.setdefault('AUTH_KEY', 'test-key')

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from src.config import settings

engine = create_engine(
    'sqlite:///:memory:',
    connect_args={'check_same_thread': False},
    poolclass=StaticPool)

# 让全局 settings 与尚未导入的业务模块默认就拿到测试引擎
settings.engine = engine


def bind_engines():
    """把已经导入、并在模块级绑定 engine 的业务模块统一指向测试引擎。"""
    import src.controllers.workflow as controllers_workflow
    import src.routes.workflow as routes_workflow
    import scripts.seed_demo as seed_demo
    import src.services.security as security
    import src.services.ingestion as ingestion
    import src.services.workflow as workflow
    controllers_workflow.engine = engine
    routes_workflow.engine = engine
    seed_demo.engine = engine
    security.engine = engine
    ingestion.engine = engine
    workflow.engine = engine
    return engine
