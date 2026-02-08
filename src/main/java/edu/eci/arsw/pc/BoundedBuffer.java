package edu.eci.arsw.pc;

import java.util.ArrayDeque;
import java.util.Deque;

/** Implementación correcta con monitores: synchronized + wait/notifyAll. */
public final class BoundedBuffer<T> {
  private final Deque<T> q = new ArrayDeque<>();
  private final int capacity;

  public BoundedBuffer(int capacity) {
    if (capacity <= 0)
      throw new IllegalArgumentException("capacity must be > 0");
    this.capacity = capacity;
  }

  public void put(T item) throws InterruptedException {
    synchronized (this) {
      while (q.size() == capacity) {
        this.wait(); // espera hasta que haya espacio
      }
      q.addLast(item);
      this.notifyAll(); // despierta consumidores
    }
  }

  public T take() throws InterruptedException {
    synchronized (this) {
      while (q.isEmpty()) {
        this.wait(); // espera hasta que haya elementos
      }
      T v = q.removeFirst();
      this.notifyAll(); // despierta productores
      return v;
    }
  }

  public synchronized int size() {
    return q.size();
  }

  public int capacity() {
    return capacity;
  }

  public synchronized T poll(){
    return q.pollFirst();
  }

  public synchronized boolean offer(T item){
    if(q.size() < capacity){
      q.addLast(item);
      return true;
    }
    return false;
  }

  public synchronized boolean isEmpty(){
    return q.isEmpty();
  }

  public synchronized boolean isFull(){
    return q.size() == capacity;
  }

}
