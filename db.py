import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) UNIQUE NOT NULL,
                full_name VARCHAR(100),
                password_hash TEXT NOT NULL,
                role VARCHAR(50) CHECK (role IN ('manager', 'expert', 'accountant', 'boss')) NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                name VARCHAR(150),
                unp VARCHAR(18) UNIQUE NOT NULL,
                contact_person VARCHAR(150),
                contact_phone VARCHAR(20),
                contact_email VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                descriptions TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals_managers (
                id SERIAL PRIMARY KEY,
                date_first_contact TIMESTAMP,
                id_users INT REFERENCES users(id),
                id_client INT REFERENCES clients(id),
                car_brand VARCHAR(150),
                sales_car VARCHAR(150),
                skp_or_bl VARCHAR(10),
                status VARCHAR(50),
                shipment_or_signing VARCHAR(10),
                prepayment VARCHAR(50),
                contract_term INT,
                currency_contract VARCHAR(10),
                interest_rate NUMERIC(5,2),
                use_number_cert INT,
                use_date_cert DATE,
                issued_number_cert INT,
                issued_date_cert DATE,
                express BOOLEAN,
                electric_car BOOLEAN,
                amount_financing NUMERIC(15,2),
                m_plan_ship DATE,
                description TEXT,
                sales_channel VARCHAR(150),
                name_agent VARCHAR(150),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS deals_expert (
                id SERIAL PRIMARY KEY,
                id_manager_deal INT REFERENCES deals_managers(id), -- Ссылка на исходную сделку
                date_appearance TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                id_manager INT REFERENCES users(id), -- ID менеджера
                id_client INT REFERENCES clients(id),
                car_brand VARCHAR(150),
                sales_car VARCHAR(150),
                skp_or_bl VARCHAR(10),
                shipment_or_signing VARCHAR(10),
                prepayment VARCHAR(50),
                contract_term INT,
                currency_contract VARCHAR(10),
                interest_rate NUMERIC(10,4),
                use_number_cert INT,
                use_date_cert DATE,
                express BOOLEAN,
                original_or_skan VARCHAR(30),
                electric_car BOOLEAN,
                solution_owner VARCHAR(30),
                date_for_ce TIMESTAMP, -- Дата для кредитного эксперта
                status VARCHAR(50),
                id_ce INT REFERENCES users(id), -- ID кредитного эксперта
                amount_financing NUMERIC(15,2),
                date_credit_committee TIMESTAMP,
                date_protocol TIMESTAMP,
                date_signing_contract TIMESTAMP,
                shipping_date TIMESTAMP, -- дата отгрузки лизинга
                expert_comment TEXT, -- Комментарий эксперта
                manager_comment TEXT, -- Комментарий менеджера
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()


def create_deal_in_db(**kwargs):
    """Создает сделку в базе данных с обработкой пустых значений"""
    # Подготавливаем данные - преобразуем пустые строки в None для числовых полей
    numeric_fields = [
        'interest_rate', 'use_number_cert', 'issued_number_cert',
        'amount_financing', 'contract_term', 'prepayment'
    ]

    for field in numeric_fields:
        if field in kwargs and kwargs[field] == '':
            kwargs[field] = None

    with connect_db() as conn, conn.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO deals_managers (
                    date_first_contact, id_users, id_client, car_brand, sales_car,
                    skp_or_bl, status, shipment_or_signing, prepayment,
                    contract_term, currency_contract, interest_rate,
                    use_number_cert, use_date_cert, issued_number_cert, issued_date_cert,
                    express, electric_car, amount_financing, m_plan_ship, description,
                    sales_channel, name_agent, created_at
                ) VALUES (
                    %(date_first_contact)s, %(user_id)s, %(client_id)s, %(car_brand)s, %(sales_car)s,
                    %(skp_or_bl)s, %(status)s, %(shipment_or_signing)s, %(prepayment)s,
                    %(contract_term)s, %(currency_contract)s, %(interest_rate)s,
                    %(use_number_cert)s, %(use_date_cert)s, %(issued_number_cert)s, %(issued_date_cert)s,
                    %(express)s, %(electric_car)s, %(amount_financing)s, %(m_plan_ship)s, %(description)s,
                    %(sales_channel)s, %(name_agent)s, CURRENT_TIMESTAMP
                )
                RETURNING id
            """, kwargs)
            new_deal_id = cur.fetchone()[0]
            conn.commit()
            return new_deal_id
        except Exception as e:
            conn.rollback()
            raise ValueError(f"Ошибка при создании сделки: {str(e)}")

def search_client(name_client):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
        SELECT id FROM clients where name = %s
        """, name_client)

        return cur.fetchone()


def get_deals_paginated(page, per_page):
    offset = (page - 1) * per_page
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.date_first_contact, u.full_name AS manager_name, c.name AS client_name,
                   d.car_brand, d.status, d.amount_financing, d.m_plan_ship
            FROM deals_managers d
            LEFT JOIN users u ON d.id_users = u.id
            LEFT JOIN clients c ON d.id_client = c.id
            ORDER BY d.id DESC
            LIMIT %s OFFSET %s;
        """, (per_page, offset))
        return cur.fetchall()


def count_deals():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(1) FROM deals_managers;")
        return cur.fetchone()[0]



def get_deal_details(deal_id):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
                d.id, d.date_first_contact, u.full_name AS manager_name, 
                c.name AS client_name, c.unp, d.car_brand, d.sales_car, 
                d.skp_or_bl, d.status, d.shipment_or_signing, 
                d.prepayment, d.contract_term, d.currency_contract, 
                d.interest_rate, d.use_number_cert, d.use_date_cert, 
                d.issued_number_cert, d.issued_date_cert, 
                d.express, d.electric_car, d.amount_financing, 
                d.m_plan_ship, d.description, d.sales_channel, d.name_agent,
                d.id_users, d.id_client
            FROM deals_managers d
            LEFT JOIN users u ON d.id_users = u.id
            LEFT JOIN clients c ON d.id_client = c.id
            WHERE d.id = %s;
        """, (deal_id,))
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


def update_or_create_expert_deal(**kwargs):
    """Обновляет существующую запись эксперта или создает новую"""
    with connect_db() as conn, conn.cursor() as cur:
        try:
            # Проверяем существование записи
            expert_deal_id = get_expert_deal_by_manager_deal_id(kwargs['id_manager_deal'])

            if expert_deal_id:
                # Обновляем существующую запись
                cur.execute("""
                    UPDATE deals_expert SET
                        car_brand = %(car_brand)s,
                        sales_car = %(sales_car)s,
                        skp_or_bl = %(skp_or_bl)s,
                        shipment_or_signing = %(shipment_or_signing)s,
                        prepayment = %(prepayment)s,
                        contract_term = %(contract_term)s,
                        currency_contract = %(currency_contract)s,
                        interest_rate = %(interest_rate)s,
                        use_number_cert = %(use_number_cert)s,
                        use_date_cert = %(use_date_cert)s,
                        express = %(express)s,
                        electric_car = %(electric_car)s,
                        status = %(status)s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %(expert_deal_id)s
                """, {**kwargs, 'expert_deal_id': expert_deal_id})
                message = "Данные эксперта успешно обновлены"
            else:
                # Создаем новую запись
                cur.execute("""
                    INSERT INTO deals_expert (
                        id_manager_deal, id_manager, id_client, car_brand, 
                        sales_car, skp_or_bl, shipment_or_signing, prepayment,
                        contract_term, currency_contract, interest_rate,
                        use_number_cert, use_date_cert, express, electric_car,
                        status
                    ) VALUES (
                        %(id_manager_deal)s, %(id_manager)s, %(id_client)s, %(car_brand)s,
                        %(sales_car)s, %(skp_or_bl)s, %(shipment_or_signing)s, %(prepayment)s,
                        %(contract_term)s, %(currency_contract)s, %(interest_rate)s,
                        %(use_number_cert)s, %(use_date_cert)s, %(express)s, %(electric_car)s,
                        %(status)s
                    )
                """, kwargs)
                message = "Сделка успешно передана эксперту"

            conn.commit()
            return {"success": True, "message": message}

        except Exception as e:
            conn.rollback()
            raise ValueError(f"Ошибка при передаче эксперту: {str(e)}")


def update_deal(deal_id, *args):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE deals_managers SET
                date_first_contact = %s,
                id_client = %s,
                car_brand = %s,
                sales_car = %s,
                skp_or_bl = %s,
                status = %s,
                shipment_or_signing = %s,
                prepayment = %s,
                contract_term = %s,
                currency_contract = %s,
                interest_rate = %s,
                use_number_cert = %s,
                use_date_cert = %s,
                issued_number_cert = %s,
                issued_date_cert = %s,
                express = %s,
                electric_car = %s,
                amount_financing = %s,
                m_plan_ship = %s,
                description = %s,
                sales_channel = %s,
                name_agent = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (*args, deal_id))
        conn.commit()

def update_deal_status(deal_id, status):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE deals_managers 
            SET status = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (status, deal_id))
        conn.commit()


# Добавляем в db.py
def create_user(username, password_hash, role, full_name):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (username, password_hash, role, full_name)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (username, password_hash, role, full_name))
        user_id = cur.fetchone()[0]
        conn.commit()
        return user_id

def get_user_by_username(username):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


def get_or_create_client(name, unp):
    """Проверяет существование клиента по УНП, если нет - создает нового"""
    with connect_db() as conn, conn.cursor() as cur:
        # Проверяем существование клиента
        cur.execute("SELECT id FROM clients WHERE unp = %s", (unp,))
        client = cur.fetchone()

        if client:
            return client[0]
        else:
            # Создаем нового клиента
            cur.execute("""
                INSERT INTO clients (name, unp) 
                VALUES (%s, %s)
                RETURNING id
            """, (name, unp))
            new_client_id = cur.fetchone()[0]
            conn.commit()
            return new_client_id


def get_expert_deal_by_manager_deal_id(manager_deal_id):
    """Получает запись эксперта по ID сделки менеджера"""
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM deals_expert 
            WHERE id_manager_deal = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (manager_deal_id,))
        result = cur.fetchone()
        return result[0] if result else None


def get_expert_deals_paginated(page, per_page, expert_id=None):
    offset = (page - 1) * per_page
    with connect_db() as conn, conn.cursor() as cur:
        query = """
            SELECT 
                e.id, e.date_appearance, 
                u.full_name AS manager_name, 
                c.name AS client_name,
                e.car_brand, e.status, 
                e.amount_financing, e.shipping_date,
                e.id_ce
            FROM deals_expert e
            LEFT JOIN users u ON e.id_manager = u.id
            LEFT JOIN clients c ON e.id_client = c.id
        """
        params = (per_page, offset)

        if expert_id:
            query += " WHERE e.id_ce = %s"
            params = (expert_id, per_page, offset)

        query += " ORDER BY e.id DESC LIMIT %s OFFSET %s;"

        cur.execute(query, params)
        return cur.fetchall()


def count_expert_deals(expert_id=None):
    with connect_db() as conn, conn.cursor() as cur:
        query = "SELECT COUNT(1) FROM deals_expert"
        params = None

        cur.execute(query, params)
        return cur.fetchone()[0]


def get_expert_deal_details(deal_id):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
                e.id, e.date_appearance, 
                u.full_name AS manager_name, 
                c.name AS client_name, c.unp,
                e.car_brand, e.sales_car, 
                e.skp_or_bl, e.status, 
                e.shipment_or_signing, e.prepayment,
                e.contract_term, e.currency_contract, 
                e.interest_rate, e.use_number_cert, 
                e.use_date_cert, e.express, 
                e.electric_car, e.amount_financing,
                e.shipping_date, e.expert_comment,
                e.manager_comment, e.id_manager_deal,
                e.original_or_skan, e.solution_owner,
                e.date_for_ce, e.date_credit_committee,
                e.date_protocol, e.date_signing_contract,
                ce.full_name AS expert_name,
                e.id_ce
            FROM deals_expert e
            LEFT JOIN users u ON e.id_manager = u.id
            LEFT JOIN clients c ON e.id_client = c.id
            LEFT JOIN users ce ON e.id_ce = ce.id
        """, (deal_id,))
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        if row:
            return dict(zip(columns, row))
        return None


def update_expert_deal(deal_id, **kwargs):
    with connect_db() as conn, conn.cursor() as cur:
        try:
            cur.execute("""
                UPDATE deals_expert SET
                    original_or_skan = %(original_or_skan)s,
                    solution_owner = %(solution_owner)s,
                    date_for_ce = %(date_for_ce)s,
                    status = %(status)s,
                    date_credit_committee = %(date_credit_committee)s,
                    date_protocol = %(date_protocol)s,
                    date_signing_contract = %(date_signing_contract)s,
                    shipping_date = %(shipping_date)s,
                    expert_comment = %(expert_comment)s,
                    id_ce = %(id_ce)s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %(deal_id)s
            """, {**kwargs, 'deal_id': deal_id})
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise ValueError(f"Ошибка при обновлении сделки эксперта: {str(e)}")


# В db.py добавляем новые функции

def get_all_clients():
    """Получает список всех клиентов"""
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, name, unp, contact_person, contact_phone, contact_email
            FROM clients
            ORDER BY name
        """)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

def get_users_by_role(role):
    """Получает пользователей по роли"""
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, username, full_name, role
            FROM users
            WHERE role = %s
            ORDER BY full_name
        """, (role,))
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]

def count_users_by_role(role):
    """Считает количество пользователей по роли"""
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM users WHERE role = %s", (role,))
        return cur.fetchone()[0]