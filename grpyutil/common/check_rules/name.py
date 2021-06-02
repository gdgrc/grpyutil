class ChineseName(object):
    def __init__(self, name):
        self._name = str(name)
        for ucode in self._name:
            assert (ucode >= '\u4e00' and ucode <= '\u9fa5') or (ucode=='·'), "name is not good %s. ucode: %s" % (self._name,ucode)

    def get_name(self):
        return self._name


if __name__ == "__main__":
    name = "嘿嘿嘿嘿嘿嘿嘿"
    name = "肉克也木·吐孙"
    ChineseName(name)


"""
判断一段文本中是否包含简体中文
import re
zhmodel = re.compile(u'[\u4e00-\u9fa5]')    #检查中文
#zhmodel = re.compile(u'[^\u4e00-\u9fa5]')   #检查非中文
contents = u'（2014）深南法民二初字第280号'
match = zhmodel.search(contents)
if match:
    print(contents)
else:

--------------------- 
作者：多动脑，多动手 
来源：CSDN 
原文：https://blog.csdn.net/wu1yr/article/details/81630494 
版权声明：本文为博主原创文章，转载请附上博文链接！
"""
