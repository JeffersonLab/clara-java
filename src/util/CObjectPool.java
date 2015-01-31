package util;

import org.jlab.coda.xmsg.excp.xMsgException;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     Object pool. This is capable of increasing its size in case there are requests and no
 *     objects are available in the pool. Class is abstract and requires a method to create
 *     an object.
 *     Second constructor takes time in seconds for periodical checking of min_number_of_objects,
 *     max_number_of_objects conditions in a separate thread. When the number of objects in the
 *     pool instances is less than min, missing instances will be created. When the number of
 *     objects in the pool is greater than max, too many instances will be removed. If the
 *     checking interval  is negative, no periodical checking of min / max conditions
 *     in a separate thread take place. These boundaries are ignored then.
 * </p>
 *
 * @author gurjyan
 * @version 1.x
 * @since 2/2/15
 */

@Deprecated
public abstract class CObjectPool<T>
{
    private ConcurrentLinkedQueue<T> pool;
    private ScheduledExecutorService executorService;

    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param mino minimum number of objects in the pool
     */
    public CObjectPool(final int mino) throws xMsgException {
        // initialize pool
        initialize(mino);
    }
    /**
     * <p>
     *     Constructor
     * </p>
     *
     * @param mino   minimum number of objects in the pool
     * @param maxo   maximum number of objects in the pool
     * @param period time in seconds for periodical checking of mino / maxo
     *               conditions in a separate thread. When the number of objects
     *               is less than mino, missing instances will be created. When
     *               the number of objects is greater than maxo, too many instances
     *               will be removed.
     */
    public CObjectPool(final int mino,
                       final int maxo,
                       final long period)
            throws xMsgException {

        // initialize pool
        initialize(mino);

        // check pool conditions in a separate thread
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run() {
                int size = pool.size();
                if (size < mino) {
                    int sizeToBeAdded = mino - size;
                    for (int i = 0; i < sizeToBeAdded; i++) {
                        try {
                            pool.add(createObject());
                        } catch (xMsgException e) {
                            e.printStackTrace();
                        }
                    }
                } else if (size > maxo) {
                    int sizeToBeRemoved = size - maxo;
                    for (int i = 0; i < sizeToBeRemoved; i++) {
                        pool.poll();
                    }
                }
            }
        }, period, period, TimeUnit.SECONDS);
    }

    /**
     * <p>
     *    Gets the next free object from the pool.
     *    If the pool doesn't contain any objects,
     *    a new object will be created and given to
     *    the caller of this method back.
     * </p>
     *
     * @return T object from the pool
     */
    public T getObject() throws xMsgException {
        T object;
        if ((object = pool.poll()) == null) {
            object = createObject();
        }

        return object;
    }

    /**
     * Returns object back to the pool.
     *
     * @param object object to be returned
     */
    public void putObject(T object) {
        if (object == null) {
            return;
        }

        this.pool.offer(object);
    }

    /**
     *  <p>
     *      Dispose the pool
     *  </p>
     */
    public void dispose() {
        if (executorService != null) {
            executorService.shutdown();
        }
        pool.clear();
    }

    /**
     * <p>
     *    Creates a new object.
     * </p>
     *
     * @return T new object
     */
    protected abstract T createObject() throws xMsgException;

    /**
     * <p>
     *     Initialize the pool with
     *     the min number of objects
     * </p>
     *
     * @param mino minimum number of objects
     */
    private void initialize(final int mino) throws xMsgException {
        pool = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < mino; i++) {
            pool.add(createObject());
        }
    }
}
