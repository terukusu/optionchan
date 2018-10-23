"""
JPXでオプション価格の更新が有った場合にオプション価格、
先物価格、現物価格をデータベースに保存するためのスクリプト
"""
import sys

from sqlalchemy import func

import jpx_loader
from webapp import db
from webapp.models import Option, FuturePriceInfo, SpotPriceInfo
from my_logging import  getLogger

log = getLogger(__name__)


def save_jpx_to_db(jpx):
    session = db.session

    log.debug('save jpx to db..')

    t = session.query(func.max(FuturePriceInfo.updated_at).label('max_updated_at')).subquery('t')
    q = session.query(FuturePriceInfo).filter(FuturePriceInfo.updated_at == t.c.max_updated_at)

    latest_future_price = q.first()

    # 同一更新時刻(priceが変化した時刻ではなく、サイトの更新時刻)の先物＆現物価格情報が
    # 存在する場合は、既存の先物＆現物価格を正とし、今回は保存しない。
    if latest_future_price is None or latest_future_price.updated_at.timestamp() != jpx.updated_at.timestamp():
        log.debug('saving future and spot price.')
        session.add(jpx.spot_price_info)
        session.add(jpx.future_price_info)
    else:
        log.debug('not saving future and spot price. already saved.')

    option_type =  next(filter(lambda o: o.is_atm, jpx.call_option_list))
    log.debug('saving call option prices. cf, atm option is: %s', option_type)
    session.add_all(jpx.call_option_list)

    option_type =  next(filter(lambda o: o.is_atm, jpx.put_option_list))
    log.debug('saving put option prices. cf, atm option is: %s', option_type)
    session.add_all(jpx.put_option_list)

    log.debug('save jpx to db..done!')

    return True


def do_import(file_path):
    session = db.session

    t = session.query(func.max(FuturePriceInfo.updated_at).label('max_updated_at')).subquery('t')
    q = session.query(FuturePriceInfo).filter(FuturePriceInfo.updated_at == t.c.max_updated_at)

    latest_future_price = q.first()

    last_price_time = None
    last_updated_at = None
    if latest_future_price is not None:
        last_price_time = latest_future_price.price_time
        last_updated_at = latest_future_price.updated_at

    log.debug('last updated_at on db: %s', last_updated_at)
    log.debug('last future price time on db: %s', last_price_time)

    if file_path:
        # 引数でHTMLファイルが指定されていればそれを読み込む
        jpx = jpx_loader.load_jpx_from_file(file_path)

        log.debug('updated_at on jpxweb: %s', jpx.updated_at)
        log.debug('future price time on jpxweb: %s', jpx.future_price_info.price_time)

        # ファイル指定の場合は更新有無の確認はせずに保存する
        # 限月の違いが有るので保存が必要かどうかを判別できないため。
        save_jpx_to_db(jpx)

    else:
        # webから読み込む

        # 期近オプション
        jpx = jpx_loader.load_jpx_nearby_month()

        log.debug('updated_at on jpxweb: %s', jpx.updated_at)
        log.debug('future price time on jpxweb: %s', jpx.future_price_info.price_time)

        is_updated = (last_price_time is None
                      or (jpx.updated_at > last_updated_at
                          and jpx.future_price_info.price_time is not None
                          and last_price_time != jpx.future_price_info.price_time))

        log.debug('is_updated: %s', is_updated)

        if is_updated:
            # 期近オプションを保存
            save_jpx_to_db(jpx)

            # 次限月オプション
            try:
                jpx = jpx_loader.load_jpx_nearby_month_2nd()
                save_jpx_to_db(jpx)
            except:
                log.warning("Unexpected error: %s", sys.exc_info()[0], exc_info=True)

            # 次の先物限月のオプション(次のMSQの月)
            try:
                jpx = jpx_loader.load_jpx_nearby_month_3rd()
                save_jpx_to_db(jpx)
            except:
                log.warning("Unexpected error: %s", sys.exc_info()[0], exc_info=True)

        else:
            log.debug('skipping..')

    session.commit()


# 溜め込んだHTMLを初期データとして投入するための特殊な関数
# file_path には JPXのHTMLが1行1ファイルで
# (期近1、次元月1、その次1、期近2、次元月2、その次2...)と並んでいる想定
def bulk_import(file_path):
    with open(file_path, mode='r') as f:
        data = f.read()

    html_list = data.split()

    session = db.session

    for i in range(0, len(html_list), 3):
        log.debug('processing file: %s', html_list[i])

        t = session.query(func.max(FuturePriceInfo.updated_at).label('max_updated_at')).subquery('t')
        q = session.query(FuturePriceInfo).filter(FuturePriceInfo.updated_at == t.c.max_updated_at)

        latest_future_price = q.first()

        last_price_time = None
        last_updated_at = None

        if latest_future_price is not None:
            last_price_time = latest_future_price.price_time
            last_updated_at = latest_future_price.updated_at

        log.debug('last updated_at on db: %s', last_updated_at)
        log.debug('last future price time on db: %s', last_price_time)

        jpx = jpx_loader.load_jpx_from_file(html_list[i])

        log.debug('updated_at on jpxweb: %s', jpx.updated_at)
        log.debug('future price time on jpxweb: %s', jpx.future_price_info.price_time)

        is_updated = (last_price_time is None
                      or (jpx.updated_at > last_updated_at
                          and jpx.future_price_info.price_time is not None
                          and last_price_time != jpx.future_price_info.price_time))

        log.debug('is_updated: %s', is_updated)

        if is_updated:
            log.debug('updating..')
            do_import(html_list[i])
            do_import(html_list[i+1])
            do_import(html_list[i+2])
        else:
            log.debug('skipping..')


if __name__ == '__main__':
    # 引数でHTMLファイルが指定されていればそれを読み込む
    file_path_arg = sys.argv[1] if len(sys.argv) >= 2 else None

    do_import(file_path_arg)

    # bulk_import(file_path_arg)
