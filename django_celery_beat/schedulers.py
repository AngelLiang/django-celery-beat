"""Beat Scheduler Implementation."""
from __future__ import absolute_import, unicode_literals

import logging

from multiprocessing.util import Finalize

from celery import current_app
from celery import schedules
from celery.beat import Scheduler, ScheduleEntry
from celery.five import values, items
from celery.utils.encoding import safe_str, safe_repr
from celery.utils.log import get_logger
from celery.utils.time import maybe_make_aware
from kombu.utils.json import dumps, loads

from django.db import transaction, close_old_connections
from django.db.utils import DatabaseError, InterfaceError
from django.core.exceptions import ObjectDoesNotExist

from .models import (
    PeriodicTask, PeriodicTasks,
    CrontabSchedule, IntervalSchedule,
    SolarSchedule,
)
from .utils import make_aware

try:
    from celery.utils.time import is_naive
except ImportError:  # pragma: no cover
    from celery.utils.timeutils import is_naive  # noqa

# This scheduler must wake up more frequently than the
# regular of 5 minutes because it needs to take external
# changes to the schedule into account.
DEFAULT_MAX_INTERVAL = 5  # seconds

ADD_ENTRY_ERROR = """\
Cannot add entry %r to database schedule: %r. Contents: %r
"""

logger = get_logger(__name__)
debug, info, warning = logger.debug, logger.info, logger.warning


class ModelEntry(ScheduleEntry):
    """Scheduler entry taken from database row.

    从数据库获取的ScheduleEntry，每一条row是一个ScheduleEntry实例。
    """

    model_schedules = (
        # (celery schedule_type, my model_type, model_field)
        (schedules.crontab, CrontabSchedule, 'crontab'),
        # bug: 下面语句的 schedules.schedule 可能应该是 schedules.interval
        (schedules.schedule, IntervalSchedule, 'interval'),
        (schedules.solar, SolarSchedule, 'solar'),
    )
    # 存储的字段
    save_fields = ['last_run_at', 'total_run_count', 'no_changes']

    def __init__(self, model, app=None):
        """Initialize the model entry.

        重写 ScheduleEntry.__init__
        """
        self.app = app or current_app._get_current_object()
        self.name = model.name
        self.task = model.task

        # 获取 model 的 schedule
        try:
            self.schedule = model.schedule
        except model.DoesNotExist:
            logger.error(
                'Disabling schedule %s that was removed from database',
                self.name,
            )
            self._disable(model)

        # 获取 model 的参数
        try:
            self.args = loads(model.args or '[]')
            self.kwargs = loads(model.kwargs or '{}')
        except ValueError as exc:
            logger.exception(
                'Removing schedule %s for argument deseralization error: %r',
                self.name, exc,
            )
            self._disable(model)

        self.options = {}
        for option in ['queue', 'exchange', 'routing_key', 'expires',
                       'priority']:
            value = getattr(model, option)
            if value is None:
                continue
            self.options[option] = value

        self.total_run_count = model.total_run_count

        # 以下属性是本类特有
        self.model = model

        # 设置 model 最后一次运行时间
        # 如果停止服务一段时间后再启动，该最后运行时间可能需要重新计算
        if not model.last_run_at:
            model.last_run_at = self._default_now()
        self.last_run_at = make_aware(model.last_run_at)

    def _disable(self, model):
        """禁用"""
        model.no_changes = True
        model.enabled = False
        model.save()

    def is_due(self):
        """是否到期
        override
        """
        # TODO:
        if not self.model.enabled:
            # 5 second delay for re-enable.
            return schedules.schedstate(False, 5.0)

        # START DATE: only run after the `start_time`, if one exists.
        if self.model.start_time is not None:
            if maybe_make_aware(self._default_now()) < self.model.start_time:
                # The datetime is before the start date - don't run.
                _, delay = self.schedule.is_due(self.last_run_at)
                # use original delay for re-check
                return schedules.schedstate(False, delay)

        # ONE OFF TASK: Disable one off tasks after they've ran once
        if self.model.one_off and self.model.enabled \
                and self.model.total_run_count > 0:
            self.model.enabled = False
            self.model.total_run_count = 0  # Reset
            self.model.no_changes = False  # Mark the model entry as changed
            self.model.save()   # 保存到数据库，需要改写
            return schedules.schedstate(False, None)  # Don't recheck

        return self.schedule.is_due(self.last_run_at)

    def _default_now(self):
        """当前默认时间"""
        now = self.app.now()
        # The PyTZ datetime must be localised for the Django-Celery-Beat
        # scheduler to work. Keep in mind that timezone arithmatic
        # with a localized timezone may be inaccurate.
        return now.tzinfo.localize(now.replace(tzinfo=None))

    def __next__(self):
        """更新下一次运行时间，并返回新的实例"""
        self.model.last_run_at = self.app.now()
        self.model.total_run_count += 1
        self.model.no_changes = True
        return self.__class__(self.model)
    next = __next__  # for 2to3

    def save(self):
        # TODO:
        # Object may not be synchronized, so only
        # change the fields we care about.
        # 对象可能没有同步，所以只修改我们关心的字段。
        obj = type(self.model)._default_manager.get(pk=self.model.pk)
        # 获取需要保存的字段并更新model
        for field in self.save_fields:
            setattr(obj, field, getattr(self.model, field))
        obj.save()

    @classmethod
    def to_model_schedule(cls, schedule):
        for schedule_type, model_type, model_field in cls.model_schedules:
            # 转换为 schedule
            schedule = schedules.maybe_schedule(schedule)
            # 如果 schedule 和 celery 的 schedule_type 匹配
            if isinstance(schedule, schedule_type):
                # 生成model
                model_schedule = model_type.from_schedule(schedule)
                model_schedule.save()   # 保存到数据库
                return model_schedule, model_field
        raise ValueError(
            'Cannot convert schedule type {0!r} to model'.format(schedule))

    @classmethod
    def from_entry(cls, name, app=None, **entry):
        """传入entry，创建实例"""
        # TODO:
        return cls(PeriodicTask._default_manager.update_or_create(
            name=name, defaults=cls._unpack_fields(**entry),
        ), app=app)

    @classmethod
    def _unpack_fields(cls, schedule,
                       args=None, kwargs=None, relative=None, options=None,
                       **entry):
        """解包成字段

        **entry sample:

            {'task': 'celery.backend_cleanup',
             'schedule': <crontab: 0 4 * * * (m/h/d/dM/MY)>,
             'options': {'expires': 43200}}

        """
        model_schedule, model_field = cls.to_model_schedule(schedule)
        entry.update(
            {model_field: model_schedule},
            args=dumps(args or []),
            kwargs=dumps(kwargs or {}),
            **cls._unpack_options(**options or {})
        )
        return entry

    @classmethod
    def _unpack_options(cls, queue=None, exchange=None, routing_key=None,
                        priority=None, **kwargs):
        """options解包成dict"""
        return {
            'queue': queue,
            'exchange': exchange,
            'routing_key': routing_key,
            'priority': priority
        }

    def __repr__(self):
        return '<ModelEntry: {0} {1}(*{2}, **{3}) {4}>'.format(
            safe_str(self.name), self.task, safe_repr(self.args),
            safe_repr(self.kwargs), self.schedule,
        )


class DatabaseScheduler(Scheduler):
    """Database-backed Beat Scheduler.

    数据库后端Baet Scheduler。
    """

    Entry = ModelEntry
    Model = PeriodicTask
    Changes = PeriodicTasks

    _schedule = None
    _last_timestamp = None
    _initial_read = True
    _heap_invalidated = False

    def __init__(self, *args, **kwargs):
        """Initialize the database scheduler."""
        self._dirty = set()
        Scheduler.__init__(self, *args, **kwargs)
        self._finalize = Finalize(self, self.sync, exitpriority=5)
        self.max_interval = (
            kwargs.get('max_interval')
            or self.app.conf.beat_max_loop_interval
            or DEFAULT_MAX_INTERVAL)

    def setup_schedule(self):
        """override"""
        self.install_default_entries(self.schedule)
        self.update_from_dict(self.app.conf.beat_schedule)

    def all_as_schedule(self):
        """加载数据库中所有的schedule"""
        debug('DatabaseScheduler: Fetching database schedule')
        s = {}
        # 遍历使能的定时任务
        for model in self.Model.objects.enabled():
            try:
                s[model.name] = self.Entry(model, app=self.app)
            except ValueError:
                pass
        return s

    def schedule_changed(self):
        """检查数据库中的schedule配置是否有变化"""
        try:
            close_old_connections()

            # If MySQL is running with transaction isolation level
            # REPEATABLE-READ (default), then we won't see changes done by
            # other transactions until the current transaction is
            # committed (Issue #41).
            try:
                transaction.commit()
            except transaction.TransactionManagementError:
                pass  # not in transaction management.

            last, ts = self._last_timestamp, self.Changes.last_change()
        except DatabaseError as exc:
            logger.exception('Database gave error: %r', exc)
            return False
        except InterfaceError:
            warning(
                'DatabaseScheduler: InterfaceError in schedule_changed(), '
                'waiting to retry in next call...'
            )
            return False

        try:
            if ts and ts > (last if last else ts):
                return True
        finally:
            self._last_timestamp = ts
        return False

    def reserve(self, entry):
        """存储entry"""
        new_entry = next(entry)
        # Need to store entry by name, because the entry may change
        # in the mean time.
        self._dirty.add(new_entry.name)
        return new_entry

    def sync(self):
        """同步，override"""
        info('Writing entries...')
        _tried = set()
        _failed = set()
        try:
            # 关闭旧连接
            close_old_connections()

            while self._dirty:
                name = self._dirty.pop()
                try:
                    # 保存到数据库，需要先实现 ModelEntry.save()
                    # self.schedule[name] 是 ModelEntry 实例
                    self.schedule[name].save()
                    _tried.add(name)
                except (KeyError, ObjectDoesNotExist) as exc:
                    _failed.add(name)
        except DatabaseError as exc:
            logger.exception('Database error while sync: %r', exc)
        except InterfaceError:
            warning(
                'DatabaseScheduler: InterfaceError in sync(), '
                'waiting to retry in next call...'
            )
        finally:
            # retry later, only for the failed ones
            self._dirty |= _failed

    def update_from_dict(self, mapping):
        """从dict更新Entry"""
        s = {}
        for name, entry_fields in items(mapping):
            try:
                entry = self.Entry.from_entry(name,
                                              app=self.app,
                                              **entry_fields)
                if entry.model.enabled:
                    s[name] = entry

            except Exception as exc:
                logger.error(ADD_ENTRY_ERROR, name, exc, entry_fields)
        self.schedule.update(s)

    def install_default_entries(self, data):
        """注册默认的Entry"""
        entries = {}
        if self.app.conf.result_expires:
            entries.setdefault(
                'celery.backend_cleanup', {
                    'task': 'celery.backend_cleanup',
                    'schedule': schedules.crontab('0', '4', '*'),
                    'options': {'expires': 12 * 3600},
                },
            )
        self.update_from_dict(entries)

    def schedules_equal(self, *args, **kwargs):
        if self._heap_invalidated:
            self._heap_invalidated = False
            return False
        return super(DatabaseScheduler, self).schedules_equal(*args, **kwargs)

    @property
    def schedule(self):
        initial = update = False
        # 第一次读取
        if self._initial_read:
            debug('DatabaseScheduler: initial read')
            initial = update = True
            self._initial_read = False
        elif self.schedule_changed():
            info('DatabaseScheduler: Schedule changed.')
            update = True

        if update:
            self.sync()
            # 从数据库获取所有的schedule
            self._schedule = self.all_as_schedule()
            # the schedule changed, invalidate the heap in Scheduler.tick
            if not initial:
                self._heap = []  # 修改了父类的 self._heap
                self._heap_invalidated = True
            if logger.isEnabledFor(logging.DEBUG):
                debug('Current schedule:\n%s', '\n'.join(
                    repr(entry) for entry in values(self._schedule)),
                )
        return self._schedule
