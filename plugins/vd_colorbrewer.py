'''Colorbrewer scales for plotting in Visidata.
   Made possible by prof. Cynthia Brewer et al. @ colorbrewer.org

   The scales were brought into Python as decimal RGB values by Github user dsc: 
   https://github.com/dsc/colorbrewer-python

   The conversion from hex RGB values to terminal colors is thanks to  MicahElliott:
   https://gist.github.com/MicahElliott/719710

   These two external scripts (colorbrewer.py and colortrans.py respectively) are fetched into the same directory as this file, which should be $HOME/.visidata/plugins/. Some rows are filtered to get rid of error messages. 

    This plugin adds two commands which can be used while viewing a plot (GraphSheet):
    "colorbrewer" lets you pick a Colorbrewer palette, then reloads the sheet
    "reset_colors" reverts to the Visidata default colors
'''

__author__='Erik Broman <mikroberna@gmail.com>'
__version__='0.1'


from visidata import vd, GraphSheet

import os
import re
import sys
import requests
import importlib.util

if not os.path.exists(os.path.join(os.getcwd(), 'colorbrewer.py')):
    url = 'https://raw.githubusercontent.com/dsc/colorbrewer-python/master/colorbrewer.py'
    c = requests.get(url, stream = True).content
    with open('colorbrewer.py', 'wb') as f:
        c = '\n'.join([l for i,l in enumerate(c.decode('utf-8').split('\n')) if i not in [173,174,356,357,358]])
        f.write(c.encode('utf-8'))
        f.close()


spec = importlib.util.spec_from_file_location('colorbrewer', os.path.join(os.getcwd(), 'colorbrewer.py'))
cb = importlib.util.module_from_spec(spec)
sys.modules['cb'] = cb
spec.loader.exec_module(cb)


if not os.path.exists(os.path.join(os.getcwd(), 'colortrans.py')):
    url = 'https://gist.githubusercontent.com/MicahElliott/719710/raw/73d047f0a3ffc35f0655488547e7f24fa3f04ea6/colortrans.py'
    c = requests.get(url, stream = True).content
    with open('colortrans.py', 'wb') as f:
        c = '\n'.join([l for i,l in enumerate(c.decode('utf-8').split('\n')) if i not in [320,321]])
        f.write(c.encode('utf-8'))
        f.close()


spec = importlib.util.spec_from_file_location('colortrans', os.path.join(os.getcwd(), 'colortrans.py'))
colortrans = importlib.util.module_from_spec(spec)
sys.modules['cb'] = colortrans
spec.loader.exec_module(colortrans)



cb_attrs = dir(cb)
for_removal = ['VERSION', 'diverging', 'qualitative', 'sequential', ]
palettes = [{'key': p} for p in cb_attrs if not p.startswith('__') and p not in for_removal]


@GraphSheet.api
def colorbrewer(sheet):
    palName = vd.choose(palettes, n = 1)
    palette = getattr(cb, palName)
    class_interval = (min(palette), max(palette))

    nClasses = int(vd.input(f'How many classes? ({class_interval[0]}-{class_interval[1]})  '))

    if nClasses < class_interval[0] or nClasses > class_interval[1]:
        vd.fail(f'Number of classes for this palette needs to be within {class_interval}')

    rgb_list = palette[nClasses]
    rgb_values = [re.findall('[0-9]{1,3}', v) for v in rgb_list]

    plot_colors = [''.join([f'{int(v[0]):02x}', f'{int(v[1]):02x}', f'{int(v[2]):02x}'])for v in rgb_values]
    plot_colors = [colortrans.rgb2short(v)[0] for v in plot_colors]
    plot_colors = ' '.join(plot_colors)
    vd.option('plot_colors', plot_colors, 'list of distinct colors to use for plotting distinct objects')
    sheet.reload()

GraphSheet.addCommand(None, 'colorbrewer', 'sheet.colorbrewer()')
GraphSheet.addCommand(None, 'reset_colors', 'vd.option("plot_colors", "green red yellow cyan magenta white 38 136 168", "list of distinct colors to use for plotting distinct objects"); sheet.reload()')


