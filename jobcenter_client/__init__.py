
# stdlib
import asyncio
import base64
import hashlib
import json
import logging
import os
#import pprint
import ssl as ssl2  # ugh
import sys
import traceback

# pypi
import pynetstring

# based on https://www.jsonrpc.org/specification
NOT_NOTIFICATION  = -32000
METHOD_ERROR      = -32001
INVALID_REQUEST   = -32600
METHOD_NOT_FOUND  = -32601
INVALID_PARAMS    = -32602
INTERNAL_ERROR    = -32603
PARSE_ERROR       = -32700

# exit codes for work() method
WORK_OK                = 0
WORK_PING_TIMEOUT      = 92
WORK_CONNECTION_CLOSED = 91

#pp = pprint.PrettyPrinter(indent=4)


class Error(Exception):
    """Base class for client errors."""


class JSON_RPC_Error(Error):
    """JSON-RPC 2.0 protocol errors."""

    def __init__(self, code, message, data=None):
        super().__init__(self)
        self.code = code
        self.message = message
        self.data = data

    # do we need repr?
    #def __repr__(self):
    #    if self.data:
    #        return "<%s %s: %s %r>" % (self.__class__.__name__, self.code,
    #                                   self.message, self.data)
    #    else:
    #        return "<%s %s: %s>" % (self.__class__.__name__, self.code,
    #                                self.message)

    def __str__(self):
        if self.data:
            return "<<%s %s: %s %r>>" % (
                self.__class__.__name__,
                self.code,
                self.message,
                self.data,
            )
        else:
            return "<<%s %s: %s>>" % (
                self.__class__.__name__,
                self.code,
                self.message,
            )


class JobCenter_Client_Error(JSON_RPC_Error):
    """errors raised by callbacks."""

    def __init__(self, message):
        super().__init__(METHOD_ERROR, message)


class JobCenter_Client:
    """todo: doc"""

    #
    # internal stuff
    #

    def __init__(
        self,
        *,
        who,
        token,
        method="password",
        host="localhost",
        port=6522,
        local_addr=None,
        tls=False,
        tls_cert=None,
        tls_key=None,
        tls_ca=None,
        tls_server_hostname=None,
        json=False,
        logger=None,
        level=None,
    ):
        self.who = who
        self.token = token
        self.method = method
        self.host = host
        self.port = port
        self.local_addr=local_addr
        self.tls=tls
        self.tls_server_hostname=tls_server_hostname
        self.json = json
        self.workername = None
        self._decoder = pynetstring.Decoder()
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(__name__)
        if level:
            self._logger.setLevel(level)
        self.__actions = {}  # announced actions
        self.__id = 0
        self.__calls ={} # low-level json-rpc calls we're waiting for
        self.__jobs = {}  # jobs we're waiting for
        self.__loop = asyncio.get_running_loop()
        self.__methods = {
            "greetings": {"cb": self._rpc_greetings, "nf": True},
            "job_done": {"cb": self._rpc_job_done, "nf": True},
            "ping": {"cb": self._rpc_ping},
            "task_ready": {"cb": self._rpc_task_ready, "nf": True},
        }
        #self.__tasks = {}
        self.tls=False
        if tls:
            self._debug("setting up tls")
            ssl_context = ssl2.create_default_context(
                purpose=ssl2.Purpose.SERVER_AUTH,
                cafile=tls_ca,
            )
            if tls_key:
                ssl_context.verify_mode = ssl2.CERT_REQUIRED
                ssl_context.load_cert_chain(
                    certfile=tls_cert,
                    keyfile=tls_key,
                )
            self.tls = ssl_context            

    def _debug(self, d):
        self._logger.debug(d)

    #
    # rpc callbacks
    #

    def _rpc_greetings(self, args):
        self._debug(f"greetings: {args!s}")
        try:
            self.__greetings.set_result(args)
        except AttributeError:
            raise Error("greetings while already connected?")

    def _rpc_job_done(self, args):
        self._debug(f'_rpc_job_done args: {args!r}')
        job_id = args["job_id"]
        outargs = args["outargs"]
        c = self.__jobs.pop(job_id, None)
        self._debug(f'_rpc_job_done c: {c!s}')
        if not c or not asyncio.isfuture(c) or c.cancelled():
            # error?
            return
        # check for future?
        if isinstance(outargs, dict) and outargs.get("error") and len(outargs) == 1:
            c.set_exception(JobCenter_Client_Error(outargs))
        else:
            c.set_result(outargs)

    def _rpc_ping(self, ping):
        return "pong"

    def _rpc_task_ready(self, args):
        actionname = args['actionname']
        job_id = args['job_id']
        a = self.__actions[actionname]
        if not a:
            self._logger.info(f"got task_ready for unknown action {actionname}")
            return
        self._debug(f"got task_ready for {actionname} job_id {job_id} calling get_task")
        f = self._call("get_task", {
            "actionname": actionname,
            "job_id": job_id,
        })
        t = asyncio.ensure_future(f)
        async def do_task(job_id2, cookie, *inargs):
            self._debug(f"in done handler coro for {job_id2}")
            oargs = {}
            try:
                if not a["addenv"]:
                    inargs=list(inargs)
                    inargs.pop()
                mode = a["mode"]
                if mode == "async":
                    oargs  = await a["cb"](job_id2, *inargs)
                elif mode == "sync":
                    oargs  = a["cb"](job_id2, *inargs)
                elif mode == "subproc":
                    # fixme: something with multiprocessing?
                    oargs = {"error": "mode subproc not yet implemented"}
                else:
                    oargs = {"error":  f"invalid mode '{mode}'"}
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                oargs = {"error": traceback.format_exception(exc_type, exc_value, exc_traceback)}
            await self._call("task_done", {
                "cookie": cookie,
                "outargs": oargs
            })
        def get_task_done(t):
            res = t.result()
            self._debug(f"in done handler for get_task {t!s} {res!s}")
            asyncio.create_task(do_task(*res))
        # add_done_callback only accepts regular funtions, so we have to do
        # the two-step indirection above to make things nicely async
        t.add_done_callback(get_task_done)

    #
    # network data handling
    #
        
    async def _handle(self):
        self._debug('in _handle')
        while True:
            data = await self.reader.read(10000)  # FIXME: read size?
            if not data:
                # eof?
                self._debug("_handle bailing out!")
                return WORK_CONNECTION_CLOSED
            # self._debug(f"_handle got data: {data!r}")
            decoded_list = self._decoder.feed(data)  # FIXME: try?
            for item in decoded_list:
                reqid = None
                try:
                    try:
                        jrpc = json.loads(item.decode())
                    except json.JSONDecodeError:
                        raise JSON_RPC_Error(PARSE_ERROR, "invalid JSON")
                    self._debug(f"R {jrpc!r}")
                    if not isinstance(jrpc, dict):
                        raise JSON_RPC_Error(INVALID_REQUEST, "not a JSON object")
                    if jrpc.get("jsonrpc", "") != "2.0":
                        raise JSON_RPC_Error(INVALID_REQUEST, "not JSON-RPC 2.0")
                    if "id" in jrpc:
                        reqid = jrpc["id"]
                        if reqid and not (
                            isinstance(reqid, str) or isinstance(reqid, int)
                        ):
                            raise JSON_RPC_Error(
                                INVALID_REQUEST, "id is not a string or number"
                            )
                    if "method" in jrpc:
                        try:
                            await self._handle_request(jrpc)
                        except JSON_RPC_Error:
                            raise
                        except Exception:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            raise JobCenter_Client_Error(traceback.format_exception(exc_type, exc_value, exc_traceback))
                    elif reqid and ("result" in jrpc or "error" in jrpc):
                        self._handle_response(jrpc)
                    else:
                        raise JSON_RPC_Error(
                            INVALID_REQUEST, "invalid JSON_RPC object!"
                        )
                except JSON_RPC_Error as err:
                    res = {
                        "jsonrpc": "2.0",
                        "id": reqid,
                        "error": {
                            "code": err.code,
                            "message": err.message
                        }
                    }
                    if err.data:
                        res["error"]["data"] = data
                    self._debug(f"E {res!r}")
                    self.writer.write(pynetstring.encode(json.dumps(res).encode()))

    async def _handle_request(self, jrpc):
        method = jrpc["method"]
        if not method in self.__methods:
            raise JSON_RPC_Error(METHOD_NOT_FOUND, f"no such method '{method}'")
        m = self.__methods[method]
        #self._debug("\n".join("{}\t{}".format(k, v) for k, v in m.items()))
        nf = m.get("nf")
        if not nf and not "id" in jrpc:
            raise JSON_RPC_Error(
                NOT_NOTIFICATION, f"method '{method}' is not a notification"
            )
        reqid = jrpc.get("id")
        if not "params" in jrpc:
            raise JSON_RPC_Error(
                INVALID_REQUEST, f"no parameters given for method '{method}'"
            )
        params = jrpc["params"]
        if not (isinstance(params, dict) or isinstance(params, list)):
            raise JSON_RPC_Error(INVALID_REQUEST, f"params should be array or object")
        ret = {
            "jsonrpc": "2.0",
            "id": reqid
        }
        ret["result"] = m["cb"](params)
        if nf:
            self._debug(f"N {ret!r}")
            return
        self._debug(f"W {ret!r}")
        self.writer.write(pynetstring.encode(json.dumps(ret).encode()))


    def _handle_response(self, jrpc):
        reqid = jrpc["id"]
        c = self.__calls.pop(reqid, None)
        self._debug(f'_handle_response c: {c!s}')
        if not c or c.cancelled():
            # error?
            return
        # check for future?
        if "result" in jrpc:
            c.set_result(jrpc["result"])
        elif "error" in jrpc:
            c.set_exception(JobCenter_Client_Error(jrpc["error"]))
        # else?

    #
    # low level call method
    #

    async def _call(self, method, params):
        # the perl-code does this:
        # my $id = md5_base64($self->{next_id}++ . $name
        #     . encode_json($args) . refaddr($cb));
        # this should be the equivalent:
        f = self.__loop.create_future()
        self.__id += 1
        reqid = (base64.b64encode(
                hashlib.md5(
                    f"{self.__id}{method}{params!s}{id(f)}".encode("utf-8")
                ).digest()
            ).decode("utf-8").strip("="))
        req = {
               "jsonrpc": "2.0",
               "method": method,
               "params": params,
               "id": reqid
            }
        self._debug(f"W {req!r}")
        self.__calls[reqid] = f
        self.writer.write(pynetstring.encode(json.dumps(req).encode()))
        return await f

    #
    # public stuff
    #

    async def connect(self):
        if self.tls: 
            self._debug(f"doing ssl {self.tls_server_hostname}")
            reader, writer = await asyncio.open_connection(
                host=self.host,
                port=self.port,
                local_addr=self.local_addr,
                ssl=self.tls,
                server_hostname=self.tls_server_hostname,
            )
        else:
            reader, writer = await asyncio.open_connection(
                host=self.host,
                port=self.port,
                local_addr=self.local_addr,
            )

        self.reader = reader
        self.writer = writer

        self.__greetings = self.__loop.create_future()
        self.__handle_task = asyncio.create_task(self._handle())

        try:
            greetings = await asyncio.wait_for(self.__greetings, timeout=10)
            del self.__greetings
        except asyncio.TimeoutError:
            raise Exception("uh? timeout or so?'") # fixme: JSON_RPC_Error?

        try:
            if greetings["who"] == "jcapi" and greetings["version"] == "1.1":
                self._logger.info(f"connected: {greetings!s}")
            else:
                raise JSON_RPC_Error(
                    f"received invalid greetings: {greetings!s}"
                )
        except KeyError:            
            raise JSON_RPC_Error(f"received weird greetings: {greetings!s}")
                
        hello = self._call(
            "hello",
            {"who":self.who, "token":self.token, "method":self.method}
        )
        self._debug("waiting for hello response")
        hello = await hello
        self._debug(f"hello: {hello!s}")
        if not hello[0]:
            raise JobCenter_Client_Error(hello[1])
        self._logger.info(f"authenticated: {hello[1]}")
        return hello[1]

    async def announce(
            self,
            *,
            actionname,
            cb,
            mode="sync",
            workername=None,
            slots=None,
            slotgroup=None,
            filter=None,
            addenv=None):
        if not self.workername:
            if workername:
                self.workername = workername
            else:
                self.workername = (
                    f"{self.who} {os.uname()[1]} {sys.argv[0]} {os.getpid()}"
                )
        if mode not in ["sync", "async", "subproc"]:
            raise JobCenter_Client_Error(f"invalid mode {mode}")
        if actionname in self.__actions:
            raise JobCenter_Client_Error(f"already announced action {actionname}?")
        res = await self._call("announce", {
            "workername": self.workername,
            "actionname": actionname,
            "slots": slots,
            "slotgroup": slotgroup,
            "filter": filter,
        })
        if not res[0]:
            raise JobCenter_Client_Error(res[1])
        self.__actions[actionname] = {
            'cb': cb,
            'mode': mode,
            'addenv': addenv,
        }
        self._debug(f"announce: {res!s}")
        return res[1]

    def call_nb(
            self,
            *,
            wfname,
            vtag=None,
            inargs=None,
            clenv=None,
            timeout=0):
        c = self._call("create_job", {
            "wfname": wfname,
            "inargs": inargs,
            "vtag": vtag,
            "clenv": clenv,
            "timeout": timeout,
        })
        t = asyncio.ensure_future(c)
        f1, f2 = self.__loop.create_future(), self.__loop.create_future()

        def done1(t):
            self._debug(f"in done handler for task {t!s}")
            job_id = None
            err = None
            try:
                job_id, err = t.result()
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                err = traceback.format_exception(exc_type, exc_value, exc_traceback)
            if job_id:
                f1.set_result(job_id)
                self.__jobs[job_id] = f2
            else:
                f1.set_exception(JobCenter_Client_Error(str(err)))
                f2.cancel()
        t.add_done_callback(done1)
        return f1, f2

    async def call(
            self,
            *,
            wfname,
            vtag=None,
            inargs=None,
            clenv=None,
            timeout=0):
        f1, f2 = self.call_nb(
            wfname=wfname,
            inargs=inargs,
            vtag=vtag,
            clenv=clenv,
            timeout=timeout)
        self._debug(f"call: {f1!s} {f2!s}")
        job_id = await f1
        self._debug(f"call: job_id {job_id!s}")
        return await f2
    
    async def work(self):
        try:
            ret = await self.__handle_task
        except asyncio.CancelledError:
            ret = 0
        return ret

    async def stop(self):
        self.__handle_task.cancel()

    async def close(self):
        self._debug("closing down..")
        self.writer.close()
        await self.writer.wait_closed()

