"""本库保留异常类型；由 collector 原样再导出。"""


class ProtocolError(Exception):
    pass


class TransferError(Exception):
    pass
