import pytest
from extractor import extract_data, extract_tags


def test_extract_data():
    test_data = '''
1：突然大量关注，在4.13号之前我们都是闷头半年自己烧钱在开发，没有拿任何PPT和空气出来ido，电报群有很多陪伴半年的。

2：关于ido，可以不用参加的，我们会拿10%收益给不持有治理代币的用户，我们的发展离不开他们，请查看白皮书

3：昨晚开会确定4.14-4.15号最低接收u500起步，地址在电报群置顶，4.15号后会不接收直接收u，4.16号开始会以白名单，积分的门槛形式进行

4：下周会上线签到，邀请，交易获取积分，突然的关注度我们也没想到

5：我们在招人，如果你有java和智能合约dex，运营，UI/UE，请联系我们。

6：我们不跑路。不浮躁，会做Web3的胖东来，要不然也不会闷头开发半年。我们会在sol和狙击枪出来后开始和kol合作大规模宣发

7：关于ido情况详细说明，我们在面前有过一次私募0.12的价格，收到3.6万u左右，目前已经不是私募价格，也不会有私募价格，全部按照ido

8：BSC链负责出块打包的交易的 48club已投。

下面是ido信息

👉第一轮IDO：0.18U(总300万)，IDO第二轮0.19U(总700万)，IDO第三轮0.2U(1000万币)

👉BSC链接收 USDT 地址：0x7900a081E80Eab4503F4B8C5Ef1537c491b522D9
0x7900a081E80Eab4503F4B8C5Ef1537c491b522D8

👉Solana链接收 USDT 地址：DVZo51LRSj97iZu7eAGTcDudjsbUs24dJ9Ha1XcABRgx

👉总量1亿，预售三轮总共2000万个币400万U以内，所有代币不锁仓ido预售结束发币，

👉市值不到600万流通只有2000万和一些空投最多，预剩余的全部锁仓，也会销毁很大一部分。详情看白皮书。

👉 如果觉得价格太高请去玩meme币，我们需要开发链上合约，交易链，还有类似电报的即时通讯，

👉请使用Web3钱包如TP okx 钱包  不要使用交易所直接转账或者提现到接收地址

我们的盈利模式如下， 但我们会把利润80%拿出来给用户，像胖东来一样。

👉1：在项目初期会对项目方免费， 在项目有一定日活和用户量后 会对项目方收费， 上logo 广告 和ave 一样

👉2：交易手续费， 和狙击枪的手续费，我们会做链上合约，借贷，我们是要打造优秀的项目。

 👉白皮书和APP在 https://t.co/SvJoVfwlqv

'''
    text_no_data = ''
    # 测试提取数据的功能
    data = extract_data(test_data)

    # 断言提取的数据是否正确
    assert data == {
        'ca': '7900a081E80Eab4503F4B8C5Ef1537c491b522D9,7900a081E80Eab4503F4B8C5Ef1537c491b522D8',
        'tg': 'https://t.co/SvJoVfwlqv'
    }
    assert extract_data(text_no_data) == {
        'ca': '',
        'tg': ''
    }


def test_extract_tags():
    test_text = 'eth,BTc,whiteList'
    result = extract_tags(test_text)

    assert result == ['whitelist']
