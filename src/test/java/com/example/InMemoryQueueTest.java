package com.example;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.concurrent.*;

public class InMemoryQueueTest {
  //
  // Implement me.
  //
	private InMemoryQueueService queueService;

	@Before
	public void setup() {
		queueService = new InMemoryQueueService();
	}
	
	@Test
	public void testPushedMessagesAreStoredToQueue() {
		String queue1 = "Test Queue 1";
		String queue2 = "Test Queue 2";
		
		queueService.push(queue1, "Test Message 1");
		queueService.push(queue1, "Test Message 2");
		queueService.push(queue2, "Test Message 3");
		
		//Makes sure correct queues have correct amount of messages
		assertEquals(2, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
	}
	
	@Test
	public void testPullReturnsSameMessage() {
		String queue1 = "Test Queue 1";
		String queue2 = "Test Queue 2";
		
		queueService.push(queue1, "Test Message 1");
		queueService.push(queue1, "Test Message 2");
		queueService.push(queue2, "Test Message 3");
		
		QueueMessage msg1 = queueService.pull(queue1);
		QueueMessage msg2 = queueService.pull(queue1);
		QueueMessage msg3 = queueService.pull(queue2);
		
		//Makes sure correct messages retrieved
		assertEquals("Test Message 1", msg1.getContent());
		assertEquals("Test Message 2", msg2.getContent());
		assertEquals("Test Message 3", msg3.getContent());
	}
	
	@Test
	public void testPullReturnsNullWhenNoQueue() throws InterruptedException {
		QueueMessage nullMsg = queueService.pull("Fake Queue");
		/*
		 * Makes sure null is received
		 */
		assertNull(nullMsg);
	}
	
	@Test
	public void testPullReturnsNullUntilVisibleMessage() throws InterruptedException {
		String queue1 = "Test Queue 1";
		
		queueService.setVisibilityTimeout(1);
		queueService.push(queue1, "Test Message 1");
		QueueMessage msg1 = queueService.pull(queue1);
		QueueMessage nullMsg = queueService.pull(queue1);
		TimeUnit.SECONDS.sleep(msg1.getVisibilityTimeout() + 1);
		QueueMessage msg2 = queueService.pull(queue1);
		
		/*
		 * Makes sure pull returns null until message becomes visible again
		 */
		assertEquals("Test Message 1", msg1.getContent());
		assertNull(nullMsg);
		assertEquals("Test Message 1", msg2.getContent());
	}
	
	@Test
	public void testDeleteRemovesMessageFromQueue() {
		String queue1 = "Test Queue 1";
		
		queueService.push(queue1, "Test Message 1");
		QueueMessage msg = queueService.pull(queue1);
		assertEquals(msg.getContent(), "Test Message 1");
		assertEquals(1, queueService.getQueueSize(queue1));
		
		queueService.delete(queue1, msg);
		/*
		 * Makes sure message is deleted from queue
		 */
		assertEquals(0, queueService.getQueueSize(queue1));
	}
	
	@Test
	public void testDeleteOnlyDeletesMessageFromCorrectQueue() {
		String queue1 = "Test Queue 1";
		String queue2 = "Test Queue 2";
		
		queueService.push(queue1, "Test Message 1");
		queueService.push(queue2, "Test Message 2");
		QueueMessage msg1 = queueService.pull(queue1);
		QueueMessage msg2 = queueService.pull(queue2);
		assertEquals(msg1.getContent(), "Test Message 1");
		assertEquals(msg2.getContent(), "Test Message 2");
		assertEquals(1, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
		
		/*
		 * Try to delete message 1 from wrong queue
		 */
		queueService.delete(queue2, msg1);
		assertEquals(1, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
		
		/*
		 * Delete message 1
		 */
		queueService.delete(queue1, msg1);
		assertEquals(0, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
		
		/*
		 * Try to delete message 2 from wrong queue
		 */
		queueService.delete(queue1, msg2);
		assertEquals(0, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
		
		/*
		 * Try to delete fake message from queue1
		 */
		queueService.delete(queue2, new QueueMessage("Fake Message", 30));
		assertEquals(0, queueService.getQueueSize(queue1));
		assertEquals(1, queueService.getQueueSize(queue2));
		
		/*
		 * Delete message 2 from correct queue
		 */
		queueService.delete(queue2, msg2);
		assertEquals(0, queueService.getQueueSize(queue1));
		assertEquals(0, queueService.getQueueSize(queue2));	
	}
	
	@Test
	public void testDeleteDoesntRemoveAfterExpiredVisibility() throws InterruptedException {
		String queue1 = "Test Queue 1";
		
		queueService.setVisibilityTimeout(1);
		
		queueService.push(queue1, "Test Message 1");
		QueueMessage msg1 = queueService.pull(queue1);
		
		TimeUnit.SECONDS.sleep(2);
		
		/*
		 * Makes sure message was not deleted
		 */
		queueService.delete(queue1, msg1);
		assertEquals(1, queueService.getQueueSize(queue1));
		
		/*
		 * Makes sure same message is able to be pulled again and deleted
		 */
		QueueMessage msg2 = queueService.pull(queue1);
		assertEquals(msg2.getContent(), "Test Message 1");
		queueService.delete(queue1, msg2);
		assertEquals(0, queueService.getQueueSize(queue1));
	}
}
