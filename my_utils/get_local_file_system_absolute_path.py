import os
import inspect


def get_absolute_path(relative_path):
    # 文件相对路径
    # relative_path = relative_path_
    # relative_path = "../data/word_re.txt"

    # 获取调用处的代码脚本所在的目录
    # 获取调用栈信息
    caller_frame = inspect.stack()[1]
    caller_script = caller_frame[0].f_globals['__file__']
    # 调用处脚本文件所在的目录
    script_directory = os.path.dirname(os.path.abspath(caller_script))

    # 拼接文件的绝对路径
    absolute_path = os.path.join(script_directory, relative_path)

    # 规范化路径--上面返回/root/project/pySpark/day01/bot_userprofile_dev/../data/word_re.txt。包含..
    absolute_path = os.path.normpath(absolute_path)
    # 返回调用处脚本文件所在的目录的上一层的data文件下的文件路径
    return "file://" + absolute_path


# print(get_absolute_path("../data/word_re.txt"))
