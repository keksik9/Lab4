import os
import sys
import logging
import requests
import argparse
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USERNAME = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Параметры VK API
VK_API_BASE_URL = "https://api.vk.com/method/"
VK_API_VERSION = "5.131"
VK_ACCESS_TOKEN = os.getenv('VK_ACCESS_TOKEN')

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))


def parse_arguments():
    parser = argparse.ArgumentParser(description="VK to Neo4j Data Processor")
    parser.add_argument('--query', type=str, help='Тип запроса для выполнения')
    parser.add_argument('--limit', type=int, default=5, help='Лимит для топовых запросов')
    parser.add_argument('--user_id', type=str, help='ID пользователя VK для обработки данных')
    parser.add_argument('--max_depth', type=int, default=2, help='Максимальная глубина обработки')
    parser.add_argument('--max_nodes', type=int, default=200, help='Максимальное количество узлов для обработки')
    return parser.parse_args()


def vk_api_call(method, params):
    params.update({
        'access_token': VK_ACCESS_TOKEN,
        'v': VK_API_VERSION,
        'lang': 'ru'
    })
    response = requests.get(VK_API_BASE_URL + method, params=params)
    if response.status_code == 200:
        result = response.json()
        if 'error' in result:
            logger.error(f"Ошибка VK API: {result['error']['error_msg']}")
            return None
        return result.get('response')
    else:
        logger.error(f"HTTP ошибка: {response.status_code} - {response.text}")
        return None


def fetch_user_info(user_id):
    params = {
        "user_ids": user_id,
        "fields": "first_name,last_name,sex,home_town,city,screen_name"
    }
    return vk_api_call("users.get", params)


def fetch_followers(user_id):
    params = {
        "user_id": user_id
    }
    return vk_api_call("users.getFollowers", params)


def fetch_followers_details(follower_ids):
    params = {
        "user_ids": ",".join(map(str, follower_ids)),
        "fields": "first_name,last_name,sex,home_town,city,screen_name"
    }
    return vk_api_call("users.get", params)


def fetch_subscriptions(user_id):
    params = {
        "user_id": user_id,
        "extended": 1
    }
    return vk_api_call("users.getSubscriptions", params)


def fetch_group_details(group_ids):
    params = {
        "group_ids": ",".join(map(str, group_ids)),
        "fields": "name,screen_name"
    }
    return vk_api_call("groups.getById", params)


def store_user(tx, user_data):
    city = user_data.get('city', {}).get('title', '')
    hometown = user_data.get('home_town', '') or city

    tx.run(
        """
        MERGE (u:User {id: $id})
        SET u.screen_name = $screen_name,
            u.name = $full_name,
            u.sex = $sex,
            u.home_town = $home_town
        """,
        id=user_data['id'],
        screen_name=user_data.get('screen_name', ''),
        full_name=f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}",
        sex=user_data.get('sex', ''),
        home_town=hometown
    )


def store_group(tx, group_data):
    tx.run(
        """
        MERGE (g:Group {id: $id})
        SET g.name = $name,
            g.screen_name = $screen_name
        """,
        id=group_data['id'],
        name=group_data.get('name', ''),
        screen_name=group_data.get('screen_name', '')
    )


def create_relationship(tx, from_id, to_id, relationship_type):
    tx.run(
        f"""
        MATCH (a {{id: $from_id}})
        MATCH (b {{id: $to_id}})
        MERGE (a)-[:{relationship_type}]->(b)
        """,
        from_id=from_id,
        to_id=to_id
    )
    logger.info(f"Создана связь {relationship_type} между {from_id} и {to_id}")


def process_network(user_id, current_level, max_depth, max_nodes=200):
    queue = [(user_id, current_level)]
    processed = set()
    node_count = 0

    while queue:
        uid, level = queue.pop(0)

        if uid in processed or level > max_depth:
            continue
        processed.add(uid)
        node_count += 1

        if node_count > max_nodes:
            logger.info("Достигнут максимальный лимит узлов.")
            break

        user_info = fetch_user_info(uid)
        if not user_info:
            logger.warning(f"Не удалось получить информацию о пользователе {uid}")
            continue
        user_details = user_info[0]

        with neo4j_driver.session() as session:
            session.write_transaction(store_user, user_details)
            logger.info(f"Добавлен пользователь {user_details['id']} на уровне {level}")

            followers = fetch_followers(uid)
            if followers:
                follower_ids = followers['items']
                followers_info = fetch_followers_details(follower_ids)
                if followers_info:
                    for follower in followers_info:
                        fid = follower['id']
                        if fid not in processed:
                            session.write_transaction(store_user, follower)
                            session.write_transaction(create_relationship, fid, uid, "FOLLOWS")
                            queue.append((fid, level + 1))
                            logger.info(f"Добавлен фолловер {fid} для пользователя {uid} на уровне {level + 1}")

            subscriptions = fetch_subscriptions(uid)
            if subscriptions and 'items' in subscriptions:
                group_ids = [item['id'] for item in subscriptions['items'] if item.get('type') == 'page']
                if group_ids:
                    groups_info = fetch_group_details(group_ids)
                    if groups_info:
                        for group in groups_info:
                            gid = group['id']
                            if gid not in processed:
                                session.write_transaction(store_group, group)
                                session.write_transaction(create_relationship, uid, gid, "SUBSCRIBED_TO")
                                logger.info(f"Добавлена подписка на группу {gid} для пользователя {uid}")

        logger.info(f"Завершена обработка уровня {level} для пользователя {uid}. Переход к уровню {level + 1}.\n")

    logger.info("Обработка завершена.")

def get_total_users(tx):
    result = tx.run("MATCH (u:User) RETURN count(u) AS total_users")
    return result.single()['total_users']


def get_total_groups(tx):
    result = tx.run("MATCH (g:Group) RETURN count(g) AS total_groups")
    return result.single()['total_groups']


def get_top_users_by_followers(tx, limit):
    query = """
    MATCH (u:User)<-[:FOLLOWS]-(f:User)
    RETURN u.name AS user_name, count(f) AS followers_count
    ORDER BY followers_count DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return result.data()


def get_top_groups(tx, limit):
    query = """
    MATCH (g:Group)<-[:SUBSCRIBED_TO]-(u:User)
    RETURN g.name AS group_name, count(u) AS subscribers_count
    ORDER BY subscribers_count DESC
    LIMIT $limit
    """
    result = tx.run(query, limit=limit)
    return result.data()


def get_mutual_followers(tx):
    query = """
    MATCH (u:User)-[:FOLLOWS]->(v:User)
    WHERE (v)-[:FOLLOWS]->(u)
    RETURN u.name AS user1, v.name AS user2
    """
    result = tx.run(query)
    return result.data()


def main():
    if not VK_ACCESS_TOKEN:
        logger.error("Токен доступа VK API не найден.")
        sys.exit(1)

    args = parse_arguments()

    if args.query:
        with neo4j_driver.session() as session:
            if args.query == 'total_users':
                total_users = session.read_transaction(get_total_users)
                print(f"Общее количество пользователей: {total_users}")
            elif args.query == 'total_groups':
                total_groups = session.read_transaction(get_total_groups)
                print(f"Общее количество групп: {total_groups}")
            elif args.query == 'top_users':
                top_users = session.read_transaction(get_top_users_by_followers, args.limit)
                print("Топ пользователей по количеству фолловеров:")
                for user in top_users:
                    print(f"{user['user_name']}: {user['followers_count']} фолловеров")
            elif args.query == 'top_groups':
                top_groups = session.read_transaction(get_top_groups, args.limit)
                print("Топ групп по количеству подписчиков:")
                for group in top_groups:
                    print(f"{group['group_name']}: {group['subscribers_count']} подписчиков")
            elif args.query == 'mutual_followers':
                mutuals = session.read_transaction(get_mutual_followers)
                print("Пары пользователей, которые являются фолловерами друг друга:")
                for pair in mutuals:
                    print(f"{pair['user1']} и {pair['user2']}")
            else:
                print("Используйте один из следующих запросов: total_users, total_groups, top_users, top_groups, mutual_followers")
        neo4j_driver.close()
        sys.exit(0)

    user_id = args.user_id or '185283514'

    user_info = fetch_user_info(user_id)
    logger.info(f"Получена информация о пользователе: {user_info}")

    if user_info:
        user_data = user_info[0]
        user_id = user_data['id']
        max_depth = args.max_depth
        max_nodes = args.max_nodes
        process_network(user_id, 0, max_depth, max_nodes)
    else:
        logger.error("Не удалось получить данные о первом пользователе.")

    neo4j_driver.close()


if __name__ == "__main__":
    main()
