from flask import Flask, request, render_template, redirect, session, jsonify, url_for
import db
import math
from auth import hash_password, verify_password
from functools import wraps

app = Flask(__name__)
app.secret_key = 'your-secret-key'  # нужно для session


# Декоратор для проверки авторизации
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


# Маршруты для авторизации
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        user = db.get_user_by_username(username)
        if user and verify_password(user['password_hash'], password):
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            return redirect(request.args.get('next') or url_for('index'))

        return render_template('login.html', error='Неверное имя пользователя или пароль')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# Главная страница
@app.route('/')
def index():
    return render_template('index.html')


# Защищенные маршруты
@app.route('/create_deal', methods=['GET', 'POST'])
@login_required
def create_deal():
    if request.method == 'POST':
        data = request.form.to_dict()

        for key in data:
            if data[key] == '':
                data[key] = None

        try:
            if not all([data.get('unp_client'), data.get('name_client')]):
                return jsonify(
                    {"success": False, "message": "УНП и Наименование клиента обязательны для заполнения"}), 400

            client_id = db.get_or_create_client(
                data.get('name_client'),
                data.get('unp_client')
            )

            deal_id = db.create_deal_in_db(
                date_first_contact=data.get('date_first_contact'),
                user_id=session.get('user_id'),
                client_id=client_id,
                car_brand=data.get('car_brand'),
                sales_car=data.get('sales_car'),
                skp_or_bl=data.get('skp_or_bl'),
                status=data.get('status', 'Согласование условий'),
                shipment_or_signing=data.get('shipment_or_signing'),
                prepayment=data.get('prepayment'),
                contract_term=data.get('contract_term'),
                currency_contract=data.get('currency_contract', 'BYN'),
                interest_rate=data.get('interest_rate'),
                use_number_cert=data.get('use_number_cert'),
                use_date_cert=data.get('use_date_cert'),
                issued_number_cert=data.get('issued_number_cert'),
                issued_date_cert=data.get('issued_date_cert'),
                express=data.get('express') == 'on',
                electric_car=data.get('electric_car') == 'on',
                amount_financing=data.get('amount_financing'),
                m_plan_ship=data.get('m_plan_ship'),
                description=data.get('description'),
                sales_channel=data.get('sales_channel'),
                name_agent=data.get('name_agent')
            )

            return jsonify({
                "success": True,
                "message": "Сделка успешно добавлена!",
                "deal_id": deal_id
            })

        except ValueError as e:
            return jsonify({"success": False, "message": str(e)}), 400
        except Exception as e:
            return jsonify({"success": False, "message": f"Внутренняя ошибка сервера: {str(e)}"}), 500

    return render_template('create_deal.html')


@app.route('/deals')
@login_required
def show_deals():
    page = int(request.args.get('page', 1))
    per_page = 10
    total_deals = db.count_deals()
    total_pages = math.ceil(total_deals / per_page)
    deals = db.get_deals_paginated(page, per_page)
    return render_template('show_deals.html', deals=deals, page=page, total_pages=total_pages)


@app.route('/deal/<int:deal_id>')
@login_required
def view_deal(deal_id):
    deal = db.get_deal_details(deal_id)
    if not deal:
        return "Сделка не найдена", 404

    deal_tuple = (
        deal['id'],
        deal['date_first_contact'],
        deal.get('manager_name'),
        deal.get('client_name'),
        deal['car_brand'],
        deal['sales_car'],
        deal['skp_or_bl'],
        deal['status'],
        deal['shipment_or_signing'],
        deal['prepayment'],
        deal['contract_term'],
        deal['currency_contract'],
        deal['interest_rate'],
        deal['use_number_cert'],
        deal['use_date_cert'],
        deal['issued_number_cert'],
        deal['issued_date_cert'],
        deal['express'],
        deal['electric_car'],
        deal['amount_financing'],
        deal['m_plan_ship'],
        deal['description'],
        deal['sales_channel'],
        deal['name_agent'],
        deal['unp']
    )

    return render_template('view_deal.html', deal=deal_tuple, can_transfer=True)


@app.route('/transfer_to_expert/<int:deal_id>', methods=['POST'])
@login_required
def transfer_to_expert(deal_id):
    try:
        # Получаем данные сделки
        deal = db.get_deal_details(deal_id)
        if not deal:
            return jsonify({"success": False, "message": "Сделка не найдена"}), 404

        # Проверяем, что сделка принадлежит текущему пользователю
        if deal['id_users'] != session['user_id']:
            return jsonify({"success": False, "message": "Вы не можете передать чужую сделку"}), 403

        # Обновляем или создаем запись эксперта
        result = db.update_or_create_expert_deal(
            id_manager_deal=deal_id,
            id_manager=deal['id_users'],
            id_client=deal['id_client'],
            car_brand=deal['car_brand'],
            sales_car=deal['sales_car'],
            skp_or_bl=deal['skp_or_bl'],
            shipment_or_signing=deal['shipment_or_signing'],
            prepayment=deal['prepayment'],
            contract_term=deal['contract_term'],
            currency_contract=deal['currency_contract'],
            interest_rate=deal['interest_rate'],
            use_number_cert=deal['use_number_cert'],
            use_date_cert=deal['use_date_cert'],
            express=deal['express'],
            electric_car=deal['electric_car'],
            status=deal['status']  # Передаем текущий статус из сделки менеджера
        )

        return jsonify(result)

    except ValueError as e:
        return jsonify({"success": False, "message": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "message": f"Внутренняя ошибка сервера: {str(e)}"}), 500


@app.route('/edit_deal/<int:deal_id>', methods=['GET', 'POST'])
@login_required
def edit_deal(deal_id):
    if request.method == 'POST':
        # AJAX-запрос (возвращаем JSON)
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            data = request.form.to_dict()

            # Очищаем данные от пустых значений
            for key in data:
                if data[key] == '':
                    data[key] = None

            try:
                deal = db.get_deal_details(deal_id)
                if not deal:
                    return jsonify({"success": False, "message": "Сделка не найдена"}), 404

                if deal['id_users'] != session['user_id']:
                    return jsonify({"success": False, "message": "Вы не можете редактировать чужую сделку"}), 403

                db.update_deal(
                    deal_id,
                    data.get('date_first_contact'),
                    deal['id_client'],
                    data.get('car_brand'),
                    data.get('sales_car'),
                    data.get('skp_or_bl'),
                    data.get('status'),
                    data.get('shipment_or_signing'),
                    data.get('prepayment'),
                    data.get('contract_term'),
                    data.get('currency_contract'),
                    data.get('interest_rate'),
                    data.get('use_number_cert'),
                    data.get('use_date_cert'),
                    data.get('issued_number_cert'),
                    data.get('issued_date_cert'),
                    data.get('express') == 'on',
                    data.get('electric_car') == 'on',
                    data.get('amount_financing'),
                    data.get('m_plan_ship'),
                    data.get('description'),
                    data.get('sales_channel'),
                    data.get('name_agent')
                )

                return jsonify({
                    "success": True,
                    "message": "Сделка успешно обновлена!",
                    "deal_id": deal_id
                })

            except ValueError as e:
                return jsonify({"success": False, "message": str(e)}), 400
            except Exception as e:
                return jsonify({"success": False, "message": f"Внутренняя ошибка сервера: {str(e)}"}), 500

        # Обычный POST-запрос (редирект)
        else:
            # ... обработка как для обычной формы ...
            return redirect(f'/deal/{deal_id}')

    # GET запрос - отображаем форму редактирования
    deal = db.get_deal_details(deal_id)
    if not deal:
        return "Сделка не найдена", 404

    if deal['id_users'] != session['user_id']:
        return "Вы не можете редактировать чужую сделку", 403

    return render_template('edit_deal.html', deal=deal)


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        full_name = request.form.get('full_name')
        role = request.form.get('role')

        if db.get_user_by_username(username):
            return render_template('register.html', error='Пользователь с таким именем уже существует')

        hashed_password = hash_password(password)
        db.create_user(username, hashed_password, role, full_name)
        return redirect(url_for('login'))

    return render_template('register.html')


@app.route('/expert/deals')
@login_required
def expert_deals():

    page = int(request.args.get('page', 1))
    per_page = 10
    total_deals = db.count_expert_deals(session['user_id'])
    total_pages = math.ceil(total_deals / per_page)
    deals = db.get_expert_deals_paginated(page, per_page)

    return render_template('expert_deals.html',
                           deals=deals,
                           page=page,
                           total_pages=total_pages,
                           user_id=session['user_id'])


@app.route('/expert/deal/<int:deal_id>')
@login_required
def expert_view_deal(deal_id):

    deal = db.get_expert_deal_details(deal_id)
    if not deal:
        return "Сделка не найдена", 404

    return render_template('expert_view_deal.html', deal=deal)


@app.route('/expert/update_deal/<int:deal_id>', methods=['POST'])
@login_required
def expert_update_deal(deal_id):
    if session.get('role') != 'expert':
        return jsonify({"success": False, "message": "Доступ запрещен"}), 403

    try:
        data = request.form.to_dict()

        # Очищаем пустые значения
        for key in data:
            if data[key] == '':
                data[key] = None

        # Добавляем ID эксперта
        data['id_ce'] = session['user_id']

        db.update_expert_deal(deal_id, **data)

        return jsonify({
            "success": True,
            "message": "Сделка успешно обновлена!",
            "deal_id": deal_id
        })

    except ValueError as e:
        return jsonify({"success": False, "message": str(e)}), 400
    except Exception as e:
        return jsonify({"success": False, "message": f"Внутренняя ошибка сервера: {str(e)}"}), 500


# В main.py добавляем новые маршруты

@app.route('/clients')
@login_required
def show_clients():
    clients = db.get_all_clients()
    return render_template('clients.html', clients=clients)


@app.route('/employees')
@login_required
def show_employees():
    experts = db.get_users_by_role('expert')
    managers = db.get_users_by_role('manager')
    accountants = db.get_users_by_role('accountant')

    stats = {
        'experts_count': db.count_users_by_role('expert'),
        'managers_count': db.count_users_by_role('manager'),
        'accountants_count': db.count_users_by_role('accountant')
    }

    return render_template('employees.html',
                           experts=experts,
                           managers=managers,
                           accountants=accountants,
                           stats=stats)


if __name__ == "__main__":
    db.init_db()
    app.run(debug=True)