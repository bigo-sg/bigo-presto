package io.prestosql.execution.executor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.Executors.defaultThreadFactory;

public final class BigoThreads
{
    private BigoThreads() {}

    /**
     * Creates a {@link ThreadFactory} that creates named threads
     * using the specified naming format.
     *
     * @param nameFormat a {@link String#format(String, Object...)}-compatible
     * format string, to which a string will be supplied as the single
     * parameter. This string will be unique to this instance of the
     * ThreadFactory and will be assigned sequentially.
     * @return the created ThreadFactory
     */
    public static ThreadFactory threadsNamed(String nameFormat, int priority)
    {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), defaultThreadFactory(), priority))
                .build();
    }

    /**
     * Creates a {@link ThreadFactory} that creates named daemon threads.
     * using the specified naming format.
     *
     * @param nameFormat see {@link #threadsNamed(String, int)}
     * @return the created ThreadFactory
     */
    public static ThreadFactory daemonThreadsNamed(String nameFormat, int priority)
    {
        return new ThreadFactoryBuilder()
                .setNameFormat(nameFormat)
                .setDaemon(true)
                .setThreadFactory(new ContextClassLoaderThreadFactory(Thread.currentThread().getContextClassLoader(), defaultThreadFactory(), priority))
                .build();
    }

    private static class ContextClassLoaderThreadFactory
            implements ThreadFactory
    {
        private final ClassLoader classLoader;
        private final ThreadFactory delegate;
        private final int priority;

        public ContextClassLoaderThreadFactory(ClassLoader classLoader, ThreadFactory delegate, int priority)
        {
            this.classLoader = classLoader;
            this.delegate = delegate;
            this.priority = priority;
        }

        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread thread = delegate.newThread(runnable);
            thread.setContextClassLoader(classLoader);
            thread.setPriority(priority);

            return thread;
        }
    }
}
