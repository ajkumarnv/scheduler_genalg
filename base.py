class SingletonMetaClass(type):
    def __init__(cls, name, bases, dict):
        super(SingletonMetaClass, cls) \
            .__init__(name, bases, dict)
        original_new = cls.__new__

        def singleton_new(cls, *args, **kwds):
            if cls.instance == None:
                cls.instance = \
                    original_new(cls, *args, **kwds)
            return cls.instance

        cls.instance = None
        cls.__new__ = staticmethod(singleton_new)


class Slot:
    def __init__(self, start_time, end_time, resource_id):
        self.start_time = start_time
        self.end_time = end_time
        self.resource_id = resource_id

    def __repr__(self):
        return f"Resource:{self.resource_id}\tstart_time:{self.start_time}\tend_time:{self.end_time}"


class Job:
    def __init__(self, id, duration, resource_requirements, priority):
        self.id = id
        self.duration = duration
        self.potential_resources = resource_requirements
        self.priority = priority


class Resource:
    def __init__(self, id):
        self.id = id
