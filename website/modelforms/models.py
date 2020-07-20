from __future__ import print_function
from django.db import models
import redis
# Create your models here.
from django.urls import reverse
from django.db import models
from jsonfield import JSONField
import collections

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
con = redis.Redis(connection_pool=pool)


class Product(models.Model):
    product_title = models.CharField(max_length=100)
    product_price = models.CharField(max_length=5)
    product_desc = models.CharField(max_length=100)

    def get_absolute_url(self):
        return reverse('modelforms:index')
