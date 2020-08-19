from mypy.plugin import FunctionContext, Plugin
from mypy.types import Type

def plugin(v):
    return ParslMypyPlugin

class ParslMypyPlugin(Plugin):
    def get_type_analyze_hook(self, t):
        # print("BENC: gtah t={}".format(t))
        return None

    def get_function_hook(self, f):
        if f == "parsl.app.errors.wrap_error":
            return wrap_error_hook
        else:
            return None

def wrap_error_hook(ctx: FunctionContext) -> Type:
    print("inside wrap_error_hook for parsl-mypy")
    print("ctx = {}".format(ctx))
    print("ctx.default_return_type = {}".format(ctx.default_return_type))
    return ctx.default_return_type
