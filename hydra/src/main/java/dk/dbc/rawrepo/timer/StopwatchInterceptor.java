/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.timer;

import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import javax.interceptor.AroundInvoke;
import javax.interceptor.Interceptor;
import javax.interceptor.InvocationContext;

@Stopwatch
@Interceptor
public class StopwatchInterceptor {
    @AroundInvoke
    public Object time(InvocationContext invocationContext) throws Exception {
        final String systemClassName = invocationContext.getMethod().getDeclaringClass().getSimpleName();
        final String systemMethodName = invocationContext.getMethod().getName();
        final Object businessCall;
        final StopWatch stopWatch = new Log4JStopWatch(systemClassName + "." + systemMethodName);
        try {
            businessCall = invocationContext.proceed();
        } finally {
            stopWatch.stop();
        }
        return businessCall;
    }
}