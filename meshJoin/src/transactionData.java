// TRANSACTIONS CLASS
import java.util.Date;

public class transactionData {
    Integer TRANSACTION_ID;
    String PRODUCT_ID;
    String CUSTOMER_ID;
    String CUSTOMER_NAME;
    String STORE_ID;
    String STORE_NAME;
    Date T_DATE;
    Integer QUANTITY;
    String PRODUCT_NAME;
    String SUPPLIER_ID;
    String SUPPLIER_NAME;
    Float TOTAL_SALE;

    public transactionData(Integer TRANSACTION_ID, String PRODUCT_ID, String CUSTOMER_ID, String CUSTOMER_NAME, String STORE_ID, String STORE_NAME, Date t_DATE, Integer QUANTITY) {
        this.TRANSACTION_ID = TRANSACTION_ID;
        this.PRODUCT_ID = PRODUCT_ID;
        this.CUSTOMER_ID = CUSTOMER_ID;
        this.CUSTOMER_NAME = CUSTOMER_NAME;
        this.STORE_ID = STORE_ID;
        this.STORE_NAME = STORE_NAME;
        T_DATE = t_DATE;
        this.QUANTITY = QUANTITY;
    }
}
