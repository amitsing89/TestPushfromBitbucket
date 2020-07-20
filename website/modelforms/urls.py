from django.conf.urls import url

from modelforms import views

app_name = 'modelforms'

urlpatterns = [

    # /modelforms/
    url(r'^$', views.IndexView.as_view(), name='index'),

    # modelforms/product/entry
    url(r'^product/entry/$',views.ProductEntry.as_view(),name='product-entry'),

    # modelforms/product/2
    url(r'^product/(?P<pk>[0-9]+)/$', views.ProductUpdate.as_view(), name='product-update'),

    # modelforms/product/(?P<pk>[0-9]+)/delete
    url(r'^album/(?P<pk>[0-9]+)/delete$', views.ProductDelete.as_view(), name='product-delete'),

]