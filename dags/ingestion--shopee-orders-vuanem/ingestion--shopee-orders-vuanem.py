import requests
import pandas as pd
import json
import hmac
import hashlib
import time
from datetime import datetime, timedelta
import pickle
import os
import concurrent.futures
from google.cloud import bigquery, storage
from google.oauth2 import service_account
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger('shopee_api')

# ==================== CẤU HÌNH ====================
PARTNER_ID = 2010730
PARTNER_KEY = "484b68514a6e6a59584a72587a557546647762635277594b4f7a634e4e594654"
HOST = "https://partner.shopeemobile.com/"

# GCS Configuration
GCS_BUCKET = "vnbi-composer-1"
GCS_PATHS = {
    "SP1": "dags/ingestion--shopee-orders-vuanem/shopee_tokens_sp1.pkl",
    "SP2": "dags/ingestion--shopee-orders-vuanem/shopee_tokens_sp2.pkl"
}
LOCAL_TEMP_DIR = "/tmp"

# CẤU HÌNH 2 TÀI KHOẢN
SHOPEE_ACCOUNTS = {
    "SP1": {
        "gcs_path": GCS_PATHS["SP1"],
        "label": "SP1"
    },
    "SP2": {
        "gcs_path": GCS_PATHS["SP2"],
        "label": "SP2"
    }
}

# BigQuery Config
BIGQUERY_CONFIG = {
    "project_id": "voltaic-country-280607",
    "dataset_id": "IP_Shopee",
    "table_id": "Shopee_Orders"
}

# ==================== GCS TOKEN MANAGEMENT ====================
def load_tokens_from_gcs(gcs_path):
    """Load tokens from GCS"""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        
        if blob.exists():
            local_temp = f"{LOCAL_TEMP_DIR}/{os.path.basename(gcs_path)}"
            blob.download_to_filename(local_temp)
            logger.info(f"Downloaded tokens from gs://{GCS_BUCKET}/{gcs_path}")
            
            with open(local_temp, 'rb') as f:
                token_data = pickle.load(f)
            
            os.remove(local_temp)
            return token_data
        else:
            logger.error(f"Token file not found in GCS: gs://{GCS_BUCKET}/{gcs_path}")
            return None
    except Exception as e:
        logger.error(f"Error loading tokens from GCS: {str(e)}")
        return None

def save_tokens_to_gcs(token_data, gcs_path):
    """Save tokens to GCS"""
    try:
        local_temp = f"{LOCAL_TEMP_DIR}/{os.path.basename(gcs_path)}"
        
        # Save locally first
        with open(local_temp, 'wb') as f:
            pickle.dump(token_data, f)
        
        # Upload to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_temp)
        
        logger.info(f"Tokens uploaded to gs://{GCS_BUCKET}/{gcs_path}")
        os.remove(local_temp)
    except Exception as e:
        logger.error(f"Error saving tokens to GCS: {str(e)}")
        raise

# ==================== AUTHENTICATION ====================
def generate_sign(api_path, timestamp, access_token="", shop_id=0):
    """Generate HMAC signature for API request"""
    if access_token and shop_id:
        base_string = f"{PARTNER_ID}{api_path}{timestamp}{access_token}{shop_id}"
    else:
        base_string = f"{PARTNER_ID}{api_path}{timestamp}"
    
    byte_key = PARTNER_KEY.encode()
    byte_data = base_string.encode()
    sign = hmac.new(byte_key, byte_data, hashlib.sha256).hexdigest()
    return sign

def refresh_token(refresh_token, shop_id):
    """Refresh access token"""
    api_path = "/api/v2/auth/access_token/get"
    timestamp = int(time.time())
    sign = generate_sign(api_path, timestamp)
    url = f"{HOST}{api_path}"
    
    params = {
        "partner_id": PARTNER_ID,
        "timestamp": timestamp,
        "sign": sign
    }
    
    data = {
        "refresh_token": refresh_token,
        "shop_id": int(shop_id),
        "partner_id": PARTNER_ID
    }
    
    try:
        response = requests.post(
            url=f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}",
            json=data,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("error") == "":
                return {
                    "access_token": result.get("access_token"),
                    "refresh_token": result.get("refresh_token"),
                    "expire_in": result.get("expire_in"),
                    "shop_id": shop_id,
                    "timestamp": int(time.time())
                }
    except Exception as e:
        logger.error(f"Error refreshing token: {str(e)}")
    
    return None

def get_valid_token(gcs_path):
    """Get valid token from GCS, refresh if needed"""
    token_data = load_tokens_from_gcs(gcs_path)
    
    if not token_data:
        logger.error(f"No token found in {gcs_path}")
        return None
    
    # Check if token needs refreshing
    current_time = int(time.time())
    token_age = current_time - token_data.get("timestamp", 0)
    expire_in = token_data.get("expire_in", 0)
    
    if token_age > (expire_in * 0.8):
        logger.info(f"Token expiring soon, refreshing...")
        new_token_data = refresh_token(
            token_data.get("refresh_token"),
            token_data.get("shop_id")
        )
        
        if new_token_data:
            save_tokens_to_gcs(new_token_data, gcs_path)
            return new_token_data
        else:
            logger.error(f"Token refresh failed")
            return None
    
    return token_data

# ==================== GET LAST REFRESH TIMESTAMP ====================
def get_last_order_refresh_timestamp():
    """
    Get the timestamp of the last order refresh from BigQuery
    Returns timestamp - 15 days for overlap
    """
    try:
        client = bigquery.Client()
        
        query = f"""
            SELECT MAX(ngay_dat_hang) as last_refresh
            FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}`
        """
        
        result = client.query(query).result()
        row = list(result)[0]
        
        if row.last_refresh:
            # Subtract 15 days for overlap
            refresh_time = int(row.last_refresh.timestamp()) - (15 * 24 * 60 * 60)
            logger.info(f"Last order refresh timestamp: {datetime.fromtimestamp(refresh_time)}")
            return refresh_time
        else:
            # Default to 30 days ago
            default_time = int(time.time()) - (30 * 24 * 60 * 60)
            logger.info(f"No previous refresh found, using default (30 days ago): {datetime.fromtimestamp(default_time)}")
            return default_time
    except Exception as e:
        logger.warning(f"Error getting last refresh timestamp: {e}")
        default_time = int(time.time()) - (30 * 24 * 60 * 60)
        logger.info(f"Error occurred, using default (30 days ago): {datetime.fromtimestamp(default_time)}")
        return default_time

# ==================== DATE RANGE SPLITTER ====================
def split_date_range(start_date, end_date, max_days=14):
    """Split date range into chunks of max_days"""
    date_ranges = []
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_days), end_date)
        date_ranges.append((current_start, current_end))
        current_start = current_end
    
    return date_ranges

# ==================== API CALLS ====================
def get_order_list(token_data, start_date, end_date):
    """Get order list for date range - using update_time like Airflow"""
    api_path = "/api/v2/order/get_order_list"
    timestamp = int(time.time())
    access_token = token_data.get("access_token")
    shop_id = int(token_data.get("shop_id"))
    sign = generate_sign(api_path, timestamp, access_token, shop_id)
    
    url = f"{HOST}{api_path}"
    
    time_from = int(start_date.timestamp())
    time_to = int(end_date.timestamp())
    
    params = {
        "partner_id": PARTNER_ID,
        "timestamp": timestamp,
        "access_token": access_token,
        "shop_id": shop_id,
        "sign": sign,
        "time_range_field": "update_time",  # ← Like Airflow: use update_time
        "time_from": time_from,
        "time_to": time_to,
        "page_size": 100,
        "cursor": "",
        "response_optional_fields": "order_status"
    }
    
    # Don't filter by order_status - get ALL
    
    all_orders = []
    more_data = True
    
    while more_data:
        try:
            if params["cursor"] != "":
                timestamp = int(time.time())
                sign = generate_sign(api_path, timestamp, access_token, shop_id)
                params["timestamp"] = timestamp
                params["sign"] = sign
            
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            request_url = f"{url}?{query_string}"
            
            response = requests.get(request_url)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("error") == "":
                    orders = result.get("response", {}).get("order_list", [])
                    all_orders.extend(orders)
                    
                    next_cursor = result.get("response", {}).get("next_cursor")
                    if next_cursor and next_cursor != params["cursor"]:
                        params["cursor"] = next_cursor
                        time.sleep(1)
                    else:
                        more_data = False
                else:
                    logger.error(f"API Error: {result.get('message')}")
                    more_data = False
            else:
                logger.error(f"HTTP Error: {response.status_code}")
                more_data = False
                
        except Exception as e:
            logger.error(f"Error fetching orders: {str(e)}")
            more_data = False
    
    return all_orders

def get_order_details(token_data, order_sn_list):
    """Get detailed order information"""
    api_path = "/api/v2/order/get_order_detail"
    
    response_optional_fields = (
        "buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,"
        "actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,"
        "pay_time,dropshipper,dropshipper_phone,split_up,buyer_cancel_reason,"
        "cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,"
        "fulfillment_flag,pickup_done_time,package_list,shipping_carrier,"
        "payment_method,total_amount,invoice_data,order_chargeable_weight_gram,"
        "voucher_code,voucher_code_by_seller,seller_voucher_code,platform_voucher_code"
    )
    
    batch_size = 50
    all_order_details = []
    
    for i in range(0, len(order_sn_list), batch_size):
        batch = order_sn_list[i:i+batch_size]
        
        timestamp = int(time.time())
        access_token = token_data.get("access_token")
        shop_id = int(token_data.get("shop_id"))
        sign = generate_sign(api_path, timestamp, access_token, shop_id)
        
        url = f"{HOST}{api_path}"
        
        params = {
            "partner_id": PARTNER_ID,
            "timestamp": timestamp,
            "access_token": access_token,
            "shop_id": shop_id,
            "sign": sign,
            "order_sn_list": ",".join(batch),
            "response_optional_fields": response_optional_fields,
        }
        
        try:
            response = requests.get(url=url, params=params)
            
            if response.status_code == 200:
                result = response.json()
                if not result.get("error"):
                    order_details = result.get("response", {}).get("order_list", [])
                    all_order_details.extend(order_details)
                else:
                    logger.error(f"API Error: {result.get('message')}")
            else:
                logger.error(f"HTTP Error: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error fetching order details: {str(e)}")
        
        if i + batch_size < len(order_sn_list):
            time.sleep(1)
    
    return all_order_details

def get_income_details(token_data, order_sn_list):
    """Get escrow/financial details for orders"""
    api_path = "/api/v2/payment/get_escrow_detail"
    
    all_escrow_details = []
    
    for i, order_sn in enumerate(order_sn_list):
        timestamp = int(time.time())
        access_token = token_data.get("access_token")
        shop_id = int(token_data.get("shop_id"))
        sign = generate_sign(api_path, timestamp, access_token, shop_id)
        
        url = f"{HOST}{api_path}"
        
        params = {
            "partner_id": PARTNER_ID,
            "timestamp": timestamp,
            "access_token": access_token,
            "shop_id": shop_id,
            "sign": sign,
            "order_sn": order_sn
        }
        
        try:
            response = requests.get(url=url, params=params)
            
            if response.status_code == 200:
                result = response.json()
                if not result.get("error"):
                    escrow_data = result.get("response", {})
                    escrow_data["order_sn"] = order_sn
                    all_escrow_details.append(escrow_data)
                    
        except Exception as e:
            pass
        
        if i < len(order_sn_list) - 1:
            time.sleep(1)
    
    return all_escrow_details

# ==================== DATA PROCESSING ====================
def process_to_dataframe(orders, order_details, income_details, san_label):
    """Process orders to DataFrame format"""
    
    order_details_dict = {order.get("order_sn"): order for order in order_details}
    income_details_dict = {}
    
    if income_details:
        for income in income_details:
            order_sn = income.get("order_sn")
            if order_sn:
                income_details_dict[order_sn] = income
    
    processed_rows = []
    
    for order in orders:
        order_sn = order.get("order_sn")
        details = order_details_dict.get(order_sn, order)
        
        item_list = details.get("item_list", [])
        
        if not item_list:
            item_list = [{}]
        
        is_first_item_for_order = True
        
        for item in item_list:
            package_list = details.get("package_list", [])
            tracking_no = package_list[0].get("tracking_number") if package_list else None
            
            escrow = income_details_dict.get(order_sn, {})
            order_income = escrow.get("order_income", {})
            items_income = order_income.get("items", [])
            
            item_income = None
            if items_income:
                model_sku = item.get("model_sku")
                item_id = item.get("item_id")
                model_id = item.get("model_id")
                
                if model_sku:
                    for income_item in items_income:
                        if income_item.get("model_sku") == model_sku:
                            item_income = income_item
                            break
                
                if not item_income and item_id:
                    for income_item in items_income:
                        if income_item.get("item_id") == item_id:
                            item_income = income_item
                            break
                
                if not item_income and model_id:
                    for income_item in items_income:
                        if income_item.get("model_id") == model_id:
                            item_income = income_item
                            break
                
                if not item_income and len(items_income) == 1:
                    item_income = items_income[0]
            
            if item_income:
                seller_discount = item_income.get("seller_discount", 0) or 0
                shopee_discount = item_income.get("shopee_discount", 0) or 0
            else:
                seller_discount = 0
                shopee_discount = 0
            
            if is_first_item_for_order:
                seller_voucher_amount = order_income.get("voucher_from_seller", 0) or 0
                shopee_voucher_amount = order_income.get("voucher_from_shopee", 0) or 0
                total_amount = details.get("total_amount")
                buyer_paid_amount = details.get("total_amount")
            else:
                seller_voucher_amount = None
                shopee_voucher_amount = None
                total_amount = None
                buyer_paid_amount = None
            
            total_seller_subsidy = seller_discount + shopee_discount
            
            row = {
                "ma_don_hang": order_sn,
                "ngay_dat_hang": datetime.fromtimestamp(details.get("create_time", 0)) if details.get("create_time") else None,
                "trang_thai_don_hang": details.get("order_status"),
                "ly_do_huy": details.get("cancel_reason") or details.get("buyer_cancel_reason"),
                "ma_van_don": tracking_no,
                "sku_san_pham": item.get("model_sku"),
                "ten_san_pham": item.get("item_name"),
                "ten_phan_loai_hang": item.get("model_name"),
                "gia_goc": item.get("model_original_price"),
                "nguoi_ban_tro_gia": seller_discount,
                "duoc_shopee_tro_gia": shopee_discount,
                "tong_so_tien_duoc_nguoi_ban_tro_gia": total_seller_subsidy,
                "gia_uu_dai": item.get("model_discounted_price"),
                "so_luong": item.get("model_quantity_purchased"),
                "tong_gia_ban_san_pham": (item.get("model_discounted_price", 0) * item.get("model_quantity_purchased", 0)) if item.get("model_discounted_price") and item.get("model_quantity_purchased") else None,
                "tong_gia_tri_don_hang": total_amount,
                "ma_giam_gia_cua_shop": seller_voucher_amount if seller_voucher_amount and seller_voucher_amount > 0 else None,
                "ma_giam_gia_cua_shopee": shopee_voucher_amount if shopee_voucher_amount and shopee_voucher_amount > 0 else None,
                "tong_so_tien_nguoi_mua_thanh_toan": buyer_paid_amount,
                "phuong_thuc_thanh_toan": details.get("payment_method"),
                "san": san_label
            }
            
            processed_rows.append(row)
            is_first_item_for_order = False
    
    df = pd.DataFrame(processed_rows)
    return df

# ==================== BIGQUERY - DELETE & INSERT ====================
def delete_and_insert_bigquery(df):
    """
    Delete orders matching order_sn, then insert new data
    Like the Airflow atomic update
    """
    try:
        client = bigquery.Client(project=BIGQUERY_CONFIG["project_id"])
        
        table_ref = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}"
        
        # Create temporary table
        timestamp = int(time.time())
        temp_table_id = f"{BIGQUERY_CONFIG['table_id']}_temp_{timestamp}"
        temp_table_ref = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{temp_table_id}"
        
        logger.info(f"Creating temporary table: {temp_table_ref}")
        
        # Upload to temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        job = client.load_table_from_dataframe(df, temp_table_ref, job_config=job_config)
        job.result()
        
        logger.info(f"Uploaded {len(df)} rows to temp table")
        
        # Delete matching orders from main table
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE ma_don_hang IN (
            SELECT DISTINCT ma_don_hang 
            FROM `{temp_table_ref}`
        )
        """
        
        client.query(delete_query).result()
        logger.info("Deleted existing records")
        
        # Insert from temp table
        insert_query = f"""
        INSERT INTO `{table_ref}`
        SELECT * FROM `{temp_table_ref}`
        """
        
        client.query(insert_query).result()
        logger.info(f"Inserted {len(df)} new records")
        
        # Clean up temp table
        client.delete_table(temp_table_ref)
        logger.info("Deleted temporary table")
        
        return True
        
    except Exception as e:
        logger.error(f"BigQuery operation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Cleanup
        try:
            client.delete_table(temp_table_ref, not_found_ok=True)
        except:
            pass
        
        return False

# ==================== PROCESS ACCOUNT (PARALLEL) ====================
def process_account(account_key, account_config, start_date, end_date):
    """Process one Shopee account"""
    logger.info(f"{'='*80}")
    logger.info(f"Processing: {account_key} ({account_config['label']})")
    logger.info(f"{'='*80}")
    
    # 1. Get token from GCS
    token_data = get_valid_token(account_config["gcs_path"])
    
    if not token_data:
        logger.error(f"Failed to authenticate {account_key}")
        return None
    
    logger.info(f"Authenticated - Shop ID: {token_data.get('shop_id')}")
    
    # 2. Get orders (split into 14-day chunks)
    all_orders = []
    date_ranges = split_date_range(start_date, end_date, max_days=14)
    
    logger.info(f"Fetching orders ({len(date_ranges)} chunks)...")
    
    for i, (range_start, range_end) in enumerate(date_ranges):
        logger.info(f"Chunk {i+1}/{len(date_ranges)}: {range_start.strftime('%Y-%m-%d')} → {range_end.strftime('%Y-%m-%d')}")
        
        orders = get_order_list(token_data, range_start, range_end)
        all_orders.extend(orders)
        
        logger.info(f"  → Found {len(orders)} orders")
        
        if i < len(date_ranges) - 1:
            time.sleep(2)
    
    if not all_orders:
        logger.warning(f"No orders found for {account_key}")
        return None
    
    logger.info(f"Total: {len(all_orders)} orders")
    
    # 3. Get order details
    logger.info("Fetching order details...")
    order_sn_list = [order.get("order_sn") for order in all_orders]
    order_details = get_order_details(token_data, order_sn_list)
    
    logger.info(f"Retrieved details for {len(order_details)} orders")
    
    # 4. Get income details
    logger.info("Fetching income details...")
    logger.info(f"  This will take ~{len(order_sn_list)} seconds")
    
    income_details = get_income_details(token_data, order_sn_list)
    
    logger.info(f"Retrieved income details for {len(income_details)} orders")
    
    # 5. Process data
    logger.info(f"Processing data for {account_key}...")
    df = process_to_dataframe(all_orders, order_details, income_details, account_config["label"])
    
    logger.info(f"Processed {len(df)} rows for {account_key}")
    
    return df

# ==================== MAIN INGESTION FUNCTION ====================
def run_shopee_ingestion(**kwargs):
    """Main ingestion function for Airflow"""
    logger.info("="*80)
    logger.info("SHOPEE ORDERS INGESTION - INCREMENTAL UPDATE")
    logger.info("="*80)
    
    # Get last refresh timestamp (like Airflow)
    last_refresh = get_last_order_refresh_timestamp()
    end_time = datetime.now()
    start_time = datetime.fromtimestamp(last_refresh)
    
    logger.info(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} → {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    all_dataframes = []
    
    # Process both accounts in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_account = {
            executor.submit(process_account, key, config, start_time, end_time): key 
            for key, config in SHOPEE_ACCOUNTS.items()
        }
        
        for future in concurrent.futures.as_completed(future_to_account):
            account_key = future_to_account[future]
            try:
                df = future.result()
                if df is not None and len(df) > 0:
                    all_dataframes.append(df)
                    logger.info(f"Completed {account_key}: {len(df)} rows")
            except Exception as e:
                logger.error(f"Error {account_key}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    # Combine and upload
    if not all_dataframes:
        logger.warning("No data to upload!")
        return True
    
    final_df = pd.concat(all_dataframes, ignore_index=True)
    
    logger.info("="*80)
    logger.info("COMBINED DATA SUMMARY")
    logger.info("="*80)
    logger.info(f"Total rows: {len(final_df)}")
    logger.info(f"Breakdown by 'san':")
    logger.info(f"\n{final_df['san'].value_counts()}")
    
    # Upload to BigQuery (delete & insert)
    logger.info("="*80)
    logger.info("UPLOADING TO BIGQUERY")
    logger.info("="*80)
    
    success = delete_and_insert_bigquery(final_df)
    
    if success:
        logger.info("="*80)
        logger.info("SUCCESS!")
        logger.info("="*80)
        logger.info(f"Total rows uploaded: {len(final_df)}")
        logger.info(f"Table: {BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_id']}.{BIGQUERY_CONFIG['table_id']}")
    else:
        raise Exception("BigQuery upload failed!")
    
    return True

# ==================== AIRFLOW DAG ====================
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingestion--shopee-orders-vuanem',
    default_args=default_args,
    description='Shopee Orders Ingestion - 2 accounts (SP1 + SP2) - Incremental Update every 2 hours',
    schedule_interval='0 */2 * * *',  # Every 2 hours
    start_date=datetime(2025, 1, 20),
    catchup=False
) as dag:
    
    shopee_orders_ingestion = PythonOperator(
        task_id='ingest_shopee_orders',
        python_callable=run_shopee_ingestion,
        dag=dag

    )

