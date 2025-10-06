import unittest
from tablefaker.streaming_server import CycleBarrier
import threading
import time


class TestCycleBarrier(unittest.TestCase):
    def test_basic_synchronization(self):
        """Test that exactly one thread gets designated to run postprocess."""
        barrier = CycleBarrier(3)
        results = []
        
        def worker():
            should_run = barrier.wait()
            results.append(should_run)
        
        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Exactly one thread should return True
        self.assertEqual(sum(results), 1)
        self.assertEqual(len(results), 3)
    
    def test_multiple_cycles(self):
        """Test barrier works across multiple cycles."""
        barrier = CycleBarrier(2)
        trigger_count = [0]
        
        def worker():
            for _ in range(3):
                if barrier.wait():
                    trigger_count[0] += 1
                time.sleep(0.1)
        
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should trigger 3 times (once per cycle)
        self.assertEqual(trigger_count[0], 3)
    
    def test_cycle_counter_increments(self):
        """Test that cycle_number increments correctly."""
        barrier = CycleBarrier(2)
        
        def worker():
            barrier.wait()
        
        # First cycle
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(barrier.cycle_number, 1)
        
        # Second cycle
        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(barrier.cycle_number, 2)


if __name__ == '__main__':
    unittest.main()