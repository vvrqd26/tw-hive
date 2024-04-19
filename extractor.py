import re


def extract_data(text):
    '''
    提取字符串中的 以太坊地址和tg链接
    '''
    ethereum_address_pattern = r'0x([A-Fa-f0-9]{40})'
    telegram_url_pattern = r'https?://t\.co/[A-Za-z0-9]+'

    # 提取以太坊地址
    ethereum_addresses = re.findall(ethereum_address_pattern, text)

    # 提取tg链接
    telegram_urls = re.findall(telegram_url_pattern, text)

    return {
        'ca': ','.join(ethereum_addresses),
        'tg': ','.join(telegram_urls)
    }


def extract_tags(text):
    '''
    提取字符串的特定标签，不区分大小写
    '''

    tags = ['whitelist', 'airdrop']

    # 匹配标签
    pattern = r'\b(' + '|'.join(tags) + r')\b'
    return [item for item in re.findall(pattern, text.lower())]
