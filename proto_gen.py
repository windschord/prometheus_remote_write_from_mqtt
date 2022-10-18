#!/usr/bin/env python
# -*- coding: utf-8 -*-
from grpc.tools import protoc

protoc.main(
    (
        '',
        '-I.',
        '--python_out=.',
        '--grpc_python_out=.',
        './prometheus.proto',
    )
)
