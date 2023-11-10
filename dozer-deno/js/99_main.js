const core = globalThis.Deno.core;
const ops = core.ops;
const internals = globalThis.__bootstrap.internals;
const primordials = globalThis.__bootstrap.primordials;
const {
    ArrayPrototypeIndexOf,
    ArrayPrototypePush,
    ArrayPrototypeShift,
    ArrayPrototypeSplice,
    DateNow,
    ErrorPrototype,
    ObjectDefineProperties,
    ObjectPrototypeIsPrototypeOf,
    ObjectSetPrototypeOf,
    SafeWeakMap,
    WeakMapPrototypeDelete,
    WeakMapPrototypeGet,
    WeakMapPrototypeSet,
} = primordials;
import * as event from "ext:deno_web/02_event.js";
import * as timers from "ext:deno_web/02_timers.js";
import {
    getDefaultInspectOptions,
    getNoColor,
    inspectArgs,
    quoteString,
} from "ext:deno_console/01_console.js";
import * as performance from "ext:deno_web/15_performance.js";
import * as fetch from "ext:deno_fetch/26_fetch.js";
import {
    mainRuntimeGlobalProperties,
    windowOrWorkerGlobalScope,
} from "ext:runtime/98_global_scope.js";

let globalThis_;

function formatException(error) {
    if (ObjectPrototypeIsPrototypeOf(ErrorPrototype, error)) {
        return null;
    } else if (typeof error == "string") {
        return `Uncaught ${inspectArgs([quoteString(error, getDefaultInspectOptions())], {
            colors: !getNoColor(),
        })
            }`;
    } else {
        return `Uncaught ${inspectArgs([error], { colors: !getNoColor() })}`;
    }
}

function runtimeStart() {
    core.setMacrotaskCallback(timers.handleTimerMacrotask);
    core.setMacrotaskCallback(promiseRejectMacrotaskCallback);
    core.setWasmStreamingCallback(fetch.handleWasmStreaming);
    core.setReportExceptionCallback(event.reportException);
    ops.op_set_format_exception_callback(formatException);
}

const pendingRejections = [];
const pendingRejectionsReasons = new SafeWeakMap();

function promiseRejectCallback(type, promise, reason) {
    switch (type) {
        case 0: {
            ops.op_store_pending_promise_rejection(promise, reason);
            ArrayPrototypePush(pendingRejections, promise);
            WeakMapPrototypeSet(pendingRejectionsReasons, promise, reason);
            break;
        }
        case 1: {
            ops.op_remove_pending_promise_rejection(promise);
            const index = ArrayPrototypeIndexOf(pendingRejections, promise);
            if (index > -1) {
                ArrayPrototypeSplice(pendingRejections, index, 1);
                WeakMapPrototypeDelete(pendingRejectionsReasons, promise);
            }
            break;
        }
        default:
            return false;
    }

    return !!globalThis_.onunhandledrejection ||
        event.listenerCount(globalThis_, "unhandledrejection") > 0 ||
        typeof internals.nodeProcessUnhandledRejectionCallback !== "undefined";
}

function promiseRejectMacrotaskCallback() {
    // We have no work to do, tell the runtime that we don't
    // need to perform microtask checkpoint.
    if (pendingRejections.length === 0) {
        return undefined;
    }

    while (pendingRejections.length > 0) {
        const promise = ArrayPrototypeShift(pendingRejections);
        const hasPendingException = ops.op_has_pending_promise_rejection(
            promise,
        );
        const reason = WeakMapPrototypeGet(pendingRejectionsReasons, promise);
        WeakMapPrototypeDelete(pendingRejectionsReasons, promise);

        if (!hasPendingException) {
            continue;
        }

        const rejectionEvent = new event.PromiseRejectionEvent(
            "unhandledrejection",
            {
                cancelable: true,
                promise,
                reason,
            },
        );

        const errorEventCb = (event) => {
            if (event.error === reason) {
                ops.op_remove_pending_promise_rejection(promise);
            }
        };
        // Add a callback for "error" event - it will be dispatched
        // if error is thrown during dispatch of "unhandledrejection"
        // event.
        globalThis_.addEventListener("error", errorEventCb);
        globalThis_.dispatchEvent(rejectionEvent);
        globalThis_.removeEventListener("error", errorEventCb);

        // If event was not yet prevented, try handing it off to Node compat layer
        // (if it was initialized)
        if (
            !rejectionEvent.defaultPrevented &&
            typeof internals.nodeProcessUnhandledRejectionCallback !== "undefined"
        ) {
            internals.nodeProcessUnhandledRejectionCallback(rejectionEvent);
        }

        // If event was not prevented (or "unhandledrejection" listeners didn't
        // throw) we will let Rust side handle it.
        if (rejectionEvent.defaultPrevented) {
            ops.op_remove_pending_promise_rejection(promise);
        }
    }
    return true;
}

delete globalThis.console;

ObjectDefineProperties(globalThis, windowOrWorkerGlobalScope);

ObjectDefineProperties(globalThis, mainRuntimeGlobalProperties);

performance.setTimeOrigin(DateNow());
globalThis_ = globalThis;

ObjectSetPrototypeOf(globalThis, Window.prototype);

event.setEventTargetData(globalThis);
event.saveGlobalThisReference(globalThis);

event.defineEventHandler(globalThis, "error");
event.defineEventHandler(globalThis, "load");
event.defineEventHandler(globalThis, "beforeunload");
event.defineEventHandler(globalThis, "unload");
event.defineEventHandler(globalThis, "unhandledrejection");

core.setPromiseRejectCallback(promiseRejectCallback);

runtimeStart();
