#coding:utf-8
from pyramid.config import Configurator
import pyramid_jinja2
import ztq_core
import os
from pyramid.authentication import AuthTktAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from ztq_console.utils import models
from ztq_console.utils.security import groupfinder
from views import MENU_CONFIG


def main(global_config, frs_root='frs', init_dispatcher_config='true', \
        frs_cache='frscache', addon_config=None, work_enable=True, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    
    # 是否启用sentinel
    enable_sentinel = settings.get('enable_sentinel', 'false').lower() == 'true'
    # 如果启用sentinel，则关于redis的host,port,db都为对sentinel的配置
    if enable_sentinel:
        # 主机列表
        hosts = settings.get('sentinel_hosts', None)
        assert(hosts)
        # sentinel的所有services name
        services = settings.get('sentinel_names', None)
        assert(services)
        # 使用的数据库
        db = int(settings.get('sentinel_db', '0'))
        assert(db >= 0)

        services = services.split(',')
        ztq_core.setup_sentinel('default', 
                map(lambda x: (x[0], int(x[1])), [host.split(':') for host in hosts.split(',')]),
                services, db = db)
        # 如果启用了sentinel
        # servers 列表为所有的 services
        MENU_CONFIG['servers'] = services
        MENU_CONFIG['current_redis'] = services[0]
        MENU_CONFIG['enable_sentinel'] = True
    else:
        # 初始化servers
        # servers 格式
        # name:host:port:db:title, ......
        servers = settings.get('servers', None)
        # servers 作为必须的配置项
        # 取消原来的redis_host,redis_port,redis_db配置
        assert(servers)
        for server in servers.split(','):
            texts = server.split(':')
            # 单个server的配置项必须介于4-5之间
            assert(len(texts) >= 4 and len(texts) <= 5)
            # 添加到待管理的服务器列表中
            MENU_CONFIG['servers'].append({
                    'name'  : texts[0],
                    'host'  : texts[1],
                    'port'  : int(texts[2]),
                    'db'    : int(texts[3]),
                    'title' : texts[4] if len(texts) == 5 else texts[0],
                })
        # 默认将列表中的第一个服务器作为默认服务器
        current_redis = MENU_CONFIG['servers'][0]
        MENU_CONFIG['current_redis'] = current_redis['name']
        #
        # 初始化Redis连接
        ztq_core.setup_redis('default'
                , current_redis['host']
                , current_redis['port'], current_redis['db'])
        MENU_CONFIG['enable_sentinel'] = False

    # 初始化权重数据数据,如果权重配置已经存在则pass
    if init_dispatcher_config.lower() == 'true':
        # init_dispatcher_config 是因为控制台可能没有运行服务， 这里去读取redis数据，会导致控制台起不来
        dispatcher_config = ztq_core.get_dispatcher_config()
        if not dispatcher_config: 
            dispatcher_config = weight = {'queue_weight':{},'worker_weight':{}}
            ztq_core.set_dispatcher_config(weight)

        queue_weight = dispatcher_config['queue_weight']
        if not queue_weight:
            queues_list = ztq_core.get_queue_config()
            for queue_name, queue_config in queues_list.items():
                queue_weight[queue_name] = queue_config.get('weight', 0)
            ztq_core.set_dispatcher_config(dispatcher_config)

    # # 开启后台服务
    # 初始化fts_web配置
    authn_policy = AuthTktAuthenticationPolicy('sosecret', callback=groupfinder, hashalg='sha512')
    authz_policy = ACLAuthorizationPolicy()
    settings = dict(settings)
    settings.setdefault('jinja2.directories', 'ztq_console:templates')
    config = Configurator(settings=settings, root_factory='ztq_console.utils.models.RootFactory')
    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(authz_policy)
    config.begin()
    config.add_renderer('.html', pyramid_jinja2.renderer_factory)
    config.add_static_view('static', 'ztq_console:static')
    config.scan('ztq_console.views')  
    config.add_route('login', '/login')
    config.add_route('logout', '/logout')
    config.add_route('password', '/password' )
    config.add_route('worker', '/worker/{id}', 
                    view='ztq_console.views.config_worker')
    config.add_route('end_thread', '/worker/{id}/{thread}/{pid}', 
                    view='ztq_console.views.stop_working_job') 
    config.add_route('taskqueue', '/taskqueues/{id}')
    config.add_route('taskqueues_config', '/taskqueues/{id}/config', 
                    view='ztq_console.views.config_queue')
    config.add_route('taskqueue_action', '/taskqueues_action/{id}')
    config.add_route('errorqueues_job', '/errorqueues/{id}/job',
                    view='ztq_console.views.error_jobs_handler')    
    config.add_route('workerlog', '/workerlog/{page}')
    config.add_route('syslog', '/syslog/{page}')
    config.add_route('errorlog', '/errorlog/{page}')
    config.add_route('errorqueue', '/errorqueue/{id}/{page}')
    config.add_route('redo_all_error_for_queue', '/redo_all_error_for_queue/{id}')
    config.add_route('del_all_error_for_queue', '/del_all_error_for_queue/{id}')
    if addon_config is not None:
        addon_config(config)
    config.end()

    return config.make_wsgi_app()

