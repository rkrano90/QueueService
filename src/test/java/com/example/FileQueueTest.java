package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

public class FileQueueTest {
  //
  // Implement me if you have time.
  //
	private FileQueueService fileQueueService;
	private static File tempDir;
	
	@BeforeClass
	public static void setupClass() {
		String tempName = "temp-dir_" + new SimpleDateFormat("ddMMyy-hhmmss-SSS").format(new Date());
		File baseDir = new File(System.getProperty("java.io.tmpdir"));
        tempDir = new File(baseDir, tempName);

        if(!tempDir.exists()) {
            tempDir.mkdir();
        }
	}
	
	@Before
	public void setup() throws IOException{
		fileQueueService = new FileQueueService(tempDir.getAbsolutePath(), 30);
	}
	
	@After
	public void deleteQueues() throws IOException{
		FileUtils.cleanDirectory(tempDir);
	}
	
	@Test
	public void testPushToFile() throws IOException{
		String queue1 = "Test Queue 1";
		
		fileQueueService.push(queue1, "msg1");
		fileQueueService.push(queue1, "msg2");
		
		/*
		 * Assert 2 messages both pushed to queue1
		 */
		assertEquals(2, fileQueueService.getQueueSize(queue1));
	}
	
	@Test
	public void testPullReturnsPushedMessages() throws IOException{
		String queue1 = "Test Queue 1";
		String queue2 = "Test Queue 2";
		
		fileQueueService.push(queue1, "msg1");
		fileQueueService.push(queue1, "msg2");
		fileQueueService.push(queue2, "msg3");
		
		QueueMessage msg1 = fileQueueService.pull(queue1);
		QueueMessage msg2 = fileQueueService.pull(queue1);
		QueueMessage msg3 = fileQueueService.pull(queue2);
		
		/*
		 * Make sure we can pull all appropriate messages from 2 queues
		 */
		assertEquals("msg1", msg1.getContent());
		assertEquals("msg2", msg2.getContent());
		assertEquals("msg3", msg3.getContent());
	}
	
	@Test
	public void testPullDoesntReturnMessageTillTimeoutIsOver() throws IOException, InterruptedException{
		String queue1 = "Test Queue 1";
		fileQueueService.setVisibilityTimeout(1);
		
		fileQueueService.push(queue1, "msg1");
		
		QueueMessage msg1 = fileQueueService.pull(queue1);
		QueueMessage nullMsg = fileQueueService.pull(queue1);
		
		TimeUnit.SECONDS.sleep(2);
		QueueMessage msg3 = fileQueueService.pull(queue1);
		
		/*
		 * Check that msg1 is pulled and can't be pulled again 
		 * until visibility timeout is over
		 */
		assertEquals("msg1", msg1.getContent());
		assertNull(nullMsg);
		assertEquals("msg1", msg3.getContent());
	}
	
	@Test
	public void testPullReturnsNullWhenNoQueue() throws IOException {
		QueueMessage nullMsg = fileQueueService.pull("fake queue");
		
		assertNull(nullMsg);
	}
	
	@Test
	public void testDeleteRemovesMessage() throws IOException, InterruptedException {
		String queue1 = "Test Queue 1";
		
		fileQueueService.setVisibilityTimeout(1);
		
		/*
		 * Asserts message is in queue
		 */
		fileQueueService.push(queue1, "msg1");
		QueueMessage msg1 = fileQueueService.pull(queue1);
		assertEquals(1, fileQueueService.getQueueSize(queue1));
		
		/*
		 * Asserts message was deleted
		 */
		fileQueueService.delete(queue1, msg1);
		assertEquals(0, fileQueueService.getQueueSize(queue1));
		
		/*
		 * Asserts message can't be pulled again
		 */
		TimeUnit.SECONDS.sleep(2);
		QueueMessage nullMsg = fileQueueService.pull(queue1);
		assertNull(nullMsg);
	}
	
	@Test
	public void testDeleteDoesntRemoveAfterTimeout() throws IOException, InterruptedException {
		String queue1 = "Test Queue 1";
		
		fileQueueService.setVisibilityTimeout(1);
		
		/*
		 * Asserts message in queue
		 */
		fileQueueService.push(queue1, "msg1");
		QueueMessage msg1 = fileQueueService.pull(queue1);
		assertEquals(1, fileQueueService.getQueueSize(queue1));
		
		/*
		 * Asserts message still on queue after delete and that 
		 * we can pull it again
		 */
		TimeUnit.SECONDS.sleep(2);
		fileQueueService.delete(queue1, msg1);
		assertEquals(1, fileQueueService.getQueueSize(queue1));
		QueueMessage msg2 = fileQueueService.pull(queue1);
		assertEquals("msg1", msg2.getContent());
	}
	
	@Test
	public void testDeleteFailsForMessageInWrongQueue() throws IOException {
		String queue1 = "Test Queue 1";
		String queue2 = "Test Queue 2";
		
		fileQueueService.push(queue1, "Test Message 1");
		fileQueueService.push(queue2, "Test Message 2");
		QueueMessage msg1 = fileQueueService.pull(queue1);
		QueueMessage msg2 = fileQueueService.pull(queue2);
		assertEquals(msg1.getContent(), "Test Message 1");
		assertEquals(msg2.getContent(), "Test Message 2");
		assertEquals(1, fileQueueService.getQueueSize(queue1));
		assertEquals(1, fileQueueService.getQueueSize(queue2));
		
		/*
		 * Try to delete message 1 from wrong queue
		 */
		fileQueueService.delete(queue2, msg1);
		assertEquals(1, fileQueueService.getQueueSize(queue1));
		assertEquals(1, fileQueueService.getQueueSize(queue2));
		
		/*
		 * Delete message 1
		 */
		fileQueueService.delete(queue1, msg1);
		assertEquals(0, fileQueueService.getQueueSize(queue1));
		assertEquals(1, fileQueueService.getQueueSize(queue2));
		
		/*
		 * Try to delete message 2 from wrong queue
		 */
		fileQueueService.delete(queue1, msg2);
		assertEquals(0, fileQueueService.getQueueSize(queue1));
		assertEquals(1, fileQueueService.getQueueSize(queue2));
		
		/*
		 * Try to delete fake message from queue1
		 */
		fileQueueService.delete(queue2, new QueueMessage("Fake Message", 30));
		assertEquals(0, fileQueueService.getQueueSize(queue1));
		assertEquals(1, fileQueueService.getQueueSize(queue2));
		
		/*
		 * Delete message 2 from correct queue
		 */
		fileQueueService.delete(queue2, msg2);
		assertEquals(0, fileQueueService.getQueueSize(queue1));
		assertEquals(0, fileQueueService.getQueueSize(queue2));	
	}
}
