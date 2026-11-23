from enum import StrEnum


class Station(StrEnum):
    EAST = 'east'
    WEST = 'west'


class BatchState(StrEnum):
    # 分段仍在汇集，允许继续补传
    GATHERING = 'gathering'
    # 收齐且无同号日期冲突，已封存为不可变整批
    SEALED = 'sealed'
    # 存在同号日期冲突，封存被拒绝，等待处置
    CONFLICT = 'conflict'


class ReplayState(StrEnum):
    # 整批回放已落库
    DONE = 'done'
    # 封存之后才到达的片段被挡在整批之外
    REJECTED_LATE = 'rejected_late'


STATION_LABELS = {Station.EAST: '东登记点', Station.WEST: '西登记点'}
BATCH_STATE_LABELS = {
    BatchState.GATHERING: '汇集中',
    BatchState.SEALED: '已封存',
    BatchState.CONFLICT: '有冲突',
}
REPLAY_LABELS = {
    ReplayState.DONE: '整批回放完成',
    ReplayState.REJECTED_LATE: '封存后迟到片段已拒收',
}
