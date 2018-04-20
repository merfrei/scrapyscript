'''
Run scrapy spiders from a script.

Blocks and runs all requests in parallel.
spiders are returned as a list.
'''

import collections
from billiard import Process  # fork of multiprocessing that works with celery

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings


class ScrapyScriptException(Exception):
    pass


class Job(object):
    '''A job is a single request to call a specific spider. *args and **kwargs
    will be passed to the spider constructor.
    '''

    def __init__(self, spider, *args, **kwargs):
        '''Parms:
          spider (spidercls): the spider to be run for this job.
        '''
        self.spider = spider
        self.args = args
        self.kwargs = kwargs


class Processor(Process):
    ''' Start a twisted reactor and run the provided scrapy spiders.
    Blocks until all have finished.
    '''

    def __init__(self, settings=None):
        '''
        Parms:
          settings (scrapy.settings.Settings) - settings to apply.  Defaults
        to Scrapy default settings.
        '''
        kwargs = {'ctx': __import__('billiard.synchronize')}

        self.settings = settings or Settings()

    def _crawl(self, requests):
        '''
        Parameters:
            requests (Request) - One or more Jobs. All will
                                 be loaded into a single invocation of the reactor.
        '''
        self.crawler = CrawlerProcess(self.settings)

        # crawl can be called multiple times to queue several requests
        for req in requests:
            self.crawler.crawl(req.spider, *req.args, **req.kwargs)

        self.crawler.start()
        self.crawler.stop()

    def run(self, jobs):
        '''Start the Scrapy engine, and execute all jobs.

        Parms:
          jobs ([Job]) - one or more Job objects to be processed.

        '''
        if not isinstance(jobs, collections.Iterable):
            jobs = [jobs]
        self.validate(jobs)

        p = Process(target=self._crawl, args=[jobs])
        p.start()

        return p

    def validate(self, jobs):
        if not all([isinstance(x, Job) for x in jobs]):
            raise ScrapyScriptException('scrapyscript requires Job objects.')
