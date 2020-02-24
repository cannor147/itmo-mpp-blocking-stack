import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class BlockingStackImpl<E> : BlockingStack<E> {
    private val head: AtomicReference<Node<E>?>
    private val elementCount: AtomicInteger
    private val segmentQueueHead: AtomicReference<SegmentQueueNode<E>>
    private val segmentQueueTail: AtomicReference<SegmentQueueNode<E>>

    init {
        val tmp = SegmentQueueNode<E>(null, AtomicReference())
        head = AtomicReference()
        elementCount = AtomicInteger()
        segmentQueueHead = AtomicReference(tmp)
        segmentQueueTail = AtomicReference(tmp)
    }

    private suspend fun suspend(): E {
        return suspendCoroutine { continuation ->
            while (true) {
                val currentTail = this.segmentQueueTail.get()
                val node = SegmentQueueNode(continuation, AtomicReference())

                if (currentTail.next.compareAndSet(null, node)) {
                    this.segmentQueueTail.compareAndSet(currentTail, node)
                    break
                }
            }
        }
    }

    private fun resume(element: E) {
        while (true) {
            val currentHead = this.segmentQueueHead.get()

            if (currentHead != this.segmentQueueTail.get() && currentHead.next.get() != null) {
                val node = currentHead.next.get()
                if (this.segmentQueueHead.compareAndSet(currentHead, node)) {
                    node!!.element?.resume(element)
                    break
                }
            }
        }
    }

    override fun push(element: E) {
        val elementCount = this.elementCount.getAndIncrement()
        if (elementCount >= 0) {
            while (true) {
                val currentHead = this.head.get()
                val node = Node(element, AtomicReference(currentHead))

                if (currentHead !== null && currentHead.element === SUSPENDED) {
                    if (this.head.compareAndSet(currentHead, currentHead.next.get())) {
                        resume(element)
                        return
                    }
                } else if (this.head.compareAndSet(currentHead, node)) {
                    return
                }
            }
        } else {
            resume(element)
        }
    }

    override suspend fun pop(): E {
        val elementCount = this.elementCount.getAndDecrement()
        if (elementCount > 0) {
            while (true) {
                val currentHead: Node<E>? = this.head.get()

                if (currentHead === null) {
                    if (this.head.compareAndSet(null, Node(SUSPENDED))) {
                        return suspend()
                    }
                } else if (this.head.compareAndSet(currentHead, currentHead.next.get())) {
                    return currentHead.element as E
                }
            }
        } else {
            return suspend()
        }
    }
}

private class SegmentQueueNode<E>(val element: Continuation<E>?, val next: AtomicReference<SegmentQueueNode<E>?>)
private class Node<E>(val element: Any?, val next: AtomicReference<Node<E>?> = AtomicReference(null))

private val SUSPENDED = Any()