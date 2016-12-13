import base64
from datetime import datetime
from pprint import pformat

import bottle
from bottle import redirect, request, route, run, template


input_form = '''
    <form action="/directory/">
        Directory address: <input name="address" type="text" value="{{address}}" />
        <input value="Show" type="submit"/>
    </form>
    <form action="./">
        <input value="Reload" type="submit"/>
    </form>
    <br />
'''

@route('/')
def index():
    return template(input_form, address='ipc:///tmp/directory')


@route('/directory/')
def dirinfo_redirect():
    address = request.query.get('address')
    if address:
        redirect('/directory/{}/'.format(base64.urlsafe_b64encode(address.encode('utf-8')).decode('utf-8')))
    else:
        redirect('/')


@route('/directory/<address>/')
def dirinfo(address):
    try:
        address = base64.urlsafe_b64decode(address).decode('utf-8')
    except Exception:
        redirect('/')

    info = get_dir_info(address)

    return template(
        input_form + '''
        {{time}}<br /><br />
        <b class="address">{{address}}</b> info<br />
        <pre>
        {{dir_info}}
        </pre>
        ''',
        time=datetime.now().strftime('%Y-%m-%d %H:%M:%S .%f'),
        address=address,
        # dir_info=json.dumps(info, indent=4, ensure_ascii=False, default=repr)
        # dir_info=repr(info)
        dir_info=pformat(info)
    )


from services.directory.client import DirectoryClient


def get_dir_info(address):
    d = DirectoryClient(address)
    return d.dump_full_info()


def main():
    run(host='localhost', port=8084, debug=True, reloader=True)
