import functools
import traceback
import os
import logging
import signal
import queue
from multiprocessing import Process, Queue
from threading import Thread

EXIT_TASK = "exit"
def deferClean(startLog=True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            logging.info("function: %s start",func.__name__)
            try:
                f =  func(*args, **kw)
                logging.info("deferClean: %s finish",func.__name__)
                return f
            except Exception as e:
                stack = traceback.format_exc()
                logging.error("defer clean func: %s exception: %s stack:\n%s",func.__name__,str(e),stack )
                p_pid = os.getppid() 
                os.kill(p_pid, signal.SIGTERM) # operation not permit?

            return

        return wrapper
    return decorator


class Task():
    def __init__(self,distributedKey,data):
        self.data = data
        self.distributedKey = distributedKey

    def getDistributedKey(self):
        return self.distributedKey

    def getData(self):
        return self.data


class producerClass():
    def __init__(self):
        pass

    def produce(self,task:Task,writeTaskQueue:Queue,writeResultQueue:Queue):
        pass

class consumerClass:
    def __init__(self):
        pass

    def init(self,firstTask):
        # raise Exception("not implement")
        pass 

    def consume(self,product):
        # raise Exception("not implement") 
        pass

    def flush(self):
        # raise Exception("not implement") 
        pass



class Parallel():
    def __init__(self,producerClass:producerClass,consumer_class:consumerClass,readNum=4):
        # queue 
        self.writeTaskQueue=Queue(maxsize=32767)
        self.readTaskQueue = Queue(maxsize =readNum*2)
        self.writeResultQueue=Queue(maxsize=32767)

        self.producerClass = producerClass
        self.readNum = readNum
        
        self.consumerClass = consumer_class
        

    def putReadTask(self,readTask):
        self.readTaskQueue.put(readTask)

    def putWriteTask(self,writeTask):
        self.writeTaskQueue.put(writeTask)

    
    def start(self):

                
        @deferClean()
        def multiThreadConsumer(fistTask:Task,queue):
            logging.info("multiThreadConsumer: "+ str(fistTask.getData()) +" consumer start")
            
            c = self.consumerClass()
            c.init(fistTask)
            #writerDw = mysql.DataWriter(sql_path,None, None, write_linenum=write_linenum)
            #writerDw.set_raw_sql(insert_sql)

            while(True):
            
                product = queue.get()
                # logging.info("get data: %s" % (product.getData()))
                if product == EXIT_TASK:
                    ret = c.flush()
                    return

                c.consume(product)


        
        def goExtractionWriter(extraction_queue:Queue):
            # logging.info("goExtractionWriter extraction  start")
            @deferClean()
            def bgWriter(extraction_queue:Queue):

                logging.info("distributed extraction bgWriter start")
                queue_class = queue.Queue
                thread_class = Thread

                thread_dict={}

             
                            
                while(True):
                    task = extraction_queue.get()

            
                    if task == EXIT_TASK:
                        # 退出所有协程 并等待退出
                        for distributed_key,v in thread_dict.items():
                            v["queue"].put(EXIT_TASK)
                            v["thread"].join()
                        
                        return


                    # logging.info("get data: "+ str(len(batch_task)))
                    # for task in batch_task:
                    distributedKey = task.getDistributedKey()
                    if distributedKey not in thread_dict:
                    

                        # 初始化线程
                        q = queue_class(maxsize=50000)
                    

                        writerP = thread_class(target=multiThreadConsumer, args=(task,q,))
                        writerP.daemon = True
                        writerP.start()

                        thread_dict[distributedKey] = {"queue":q,"thread":writerP}

                    # logging.info("put data: %s" % (raw_data))
                    thread_dict[distributedKey]["queue"].put(task)

            
                
                

        
            p = Process(target=bgWriter, args=(extraction_queue,))
            p.daemon = True
            p.start()
        
            return p

    
        @deferClean()
        def read_extraction(no):
            logging.info("read process start: %d" % (no))
            while(True):
                task = self.readTaskQueue.get()
                if task == EXIT_TASK:
                    self.writeResultQueue.put(0)
                    return
                p = self.producerClass()
                p.produce(task,self.writeTaskQueue,self.writeResultQueue)

                



        # 建立读进程组
        read_process_list = []
        for no in range(self.readNum):
            p = Process(target=read_extraction, args=(no,))
            p.daemon = True
            p.start()
            read_process_list.append(p)

        self.readProcessList = read_process_list

        database_process = goExtractionWriter(self.writeTaskQueue)

        process_list = [database_process]


        process_list.extend(self.readProcessList)
        self.totalProcessList  = process_list

        logging.info("total process length: %d" % len(self.totalProcessList))

    def waitFinish(self):
        self.endReadProcessAndWait()
        self.endWriteProcessListAndWait()


    def endReadProcessAndWait(self):
        # 结束读进程生命
        for no in range(args.rpn):
            self.readTaskQueue.put(EXIT_TASK)

        # 等待读进程结束
        for p in self.readProcessList:
            p.join()

    def endWriteProcessListAndWait(self):
        # 结束写进程
        self.writeTaskQueue.put(EXIT_TASK)
        # wait to stop
        for p in self.writeProcessList:
            p.join()

   

    




    

        


    


