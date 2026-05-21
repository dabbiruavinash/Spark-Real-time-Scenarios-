you are tasked with designing a simple banking system using python
implement a bankaccount that supports:
creating an account with account_holder name and an initial balance
methods to deposit and withdraw money
a method get_balance() to check the current balance
in addition to the above , implement the following:
decorator requirement:
create a decorator called log_transaction that logs the transaction type (deposit or withdrawal), the amount and the balance after the transaction. This decorator should be applied to both deposit and withdraw methods.

from functools import wraps
from datetime import datetime
from typing import Optional, Dict, List
import json

# ========== 1. Log Transaction Decorator ==========
def log_transaction(func):
    """
    Decorator that logs transaction details including:
    - Transaction type (deposit/withdrawal)
    - Amount
    - Balance after transaction
    - Timestamp
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Get transaction amount from args or kwargs
        amount = args[0] if args else kwargs.get('amount', 0)
        
        # Capture balance before transaction
        balance_before = self._balance
        
        # Execute the actual transaction method
        result = func(self, *args, **kwargs)
        
        # Log the transaction
        transaction_log = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "account_holder": self.account_holder,
            "transaction_type": func.__name__.upper(),
            "amount": amount,
            "balance_before": balance_before,
            "balance_after": self._balance,
            "status": "SUCCESS" if result else "FAILED"
        }
        
        # Store log in account's transaction history
        self.transaction_history.append(transaction_log)
        
        # Print formatted log
        print(f"\n📝 TRANSACTION LOG:")
        print(f"   Time: {transaction_log['timestamp']}")
        print(f"   Type: {transaction_log['transaction_type']}")
        print(f"   Amount: ${amount:,.2f}")
        print(f"   Balance After: ${self._balance:,.2f}")
        print(f"   Status: {transaction_log['status']}")
        
        return result
    return wrapper


# ========== 2. Custom Exceptions ==========
class InsufficientFundsError(Exception):
    """Raised when withdrawal amount exceeds available balance"""
    pass

class InvalidAmountError(Exception):
    """Raised when transaction amount is invalid (negative or zero)"""
    pass


# ========== 3. BankAccount Class ==========
class BankAccount:
    """
    A simple banking system supporting deposits, withdrawals, and balance checks.
    
    Features:
    - Create account with holder name and initial balance
    - Deposit money (with logging)
    - Withdraw money (with logging & balance validation)
    - Check current balance
    - Transaction history tracking
    """
    
    def __init__(self, account_holder: str, initial_balance: float = 0.0):
        """
        Initialize a new bank account.
        
        Args:
            account_holder: Name of the account holder
            initial_balance: Starting balance (default 0.0)
        """
        if initial_balance < 0:
            raise InvalidAmountError("Initial balance cannot be negative")
        
        self.account_holder = account_holder
        self._balance = float(initial_balance)
        self.account_number = self._generate_account_number()
        self.transaction_history: List[Dict] = []
        
        print(f"✅ Account created successfully!")
        print(f"   Account Holder: {self.account_holder}")
        print(f"   Account Number: {self.account_number}")
        print(f"   Initial Balance: ${self._balance:,.2f}")
    
    @staticmethod
    def _generate_account_number() -> str:
        """Generate a simple unique account number"""
        import random
        return f"ACC{random.randint(10000, 99999)}"
    
    @log_transaction
    def deposit(self, amount: float) -> bool:
        """
        Deposit money into the account.
        
        Args:
            amount: Amount to deposit (must be positive)
        
        Returns:
            bool: True if deposit successful
            
        Raises:
            InvalidAmountError: If amount is not positive
        """
        if amount <= 0:
            raise InvalidAmountError(f"Deposit amount must be positive. Got: ${amount}")
        
        self._balance += amount
        return True
    
    @log_transaction
    def withdraw(self, amount: float) -> bool:
        """
        Withdraw money from the account.
        
        Args:
            amount: Amount to withdraw (must be positive and <= balance)
        
        Returns:
            bool: True if withdrawal successful
            
        Raises:
            InvalidAmountError: If amount is not positive
            InsufficientFundsError: If insufficient balance
        """
        if amount <= 0:
            raise InvalidAmountError(f"Withdrawal amount must be positive. Got: ${amount}")
        
        if amount > self._balance:
            raise InsufficientFundsError(
                f"Insufficient funds. Available: ${self._balance:,.2f}, Requested: ${amount:,.2f}"
            )
        
        self._balance -= amount
        return True
    
    def get_balance(self) -> float:
        """
        Check current account balance.
        
        Returns:
            float: Current balance
        """
        print(f"\n💰 Balance for {self.account_holder} (ACC: {self.account_number}): ${self._balance:,.2f}")
        return self._balance
    
    def get_transaction_history(self) -> List[Dict]:
        """
        Retrieve complete transaction history.
        
        Returns:
            List[Dict]: List of all transactions with details
        """
        if not self.transaction_history:
            print("\n📭 No transactions yet.")
            return []
        
        print(f"\n📜 TRANSACTION HISTORY for {self.account_holder}:")
        print("-" * 80)
        for transaction in self.transaction_history:
            print(f"  {transaction['timestamp']} | {transaction['transaction_type']:10} | "
                  f"${transaction['amount']:10,.2f} | Balance: ${transaction['balance_after']:10,.2f} | {transaction['status']}")
        print("-" * 80)
        return self.transaction_history
    
    def get_statement(self) -> Dict:
        """
        Generate account statement summary.
        
        Returns:
            Dict: Account summary including transaction statistics
        """
        total_deposits = sum(t['amount'] for t in self.transaction_history 
                            if t['transaction_type'] == 'DEPOSIT')
        total_withdrawals = sum(t['amount'] for t in self.transaction_history 
                               if t['transaction_type'] == 'WITHDRAW')
        
        statement = {
            "account_holder": self.account_holder,
            "account_number": self.account_number,
            "current_balance": self._balance,
            "total_deposits": total_deposits,
            "total_withdrawals": total_withdrawals,
            "total_transactions": len(self.transaction_history),
            "transaction_summary": {
                "deposits": len([t for t in self.transaction_history if t['transaction_type'] == 'DEPOSIT']),
                "withdrawals": len([t for t in self.transaction_history if t['transaction_type'] == 'WITHDRAW'])
            }
        }
        
        print(f"\n📊 ACCOUNT STATEMENT")
        print("=" * 50)
        print(f"Account Holder: {statement['account_holder']}")
        print(f"Account Number: {statement['account_number']}")
        print(f"Current Balance: ${statement['current_balance']:,.2f}")
        print(f"Total Deposits: ${statement['total_deposits']:,.2f}")
        print(f"Total Withdrawals: ${statement['total_withdrawals']:,.2f}")
        print(f"Total Transactions: {statement['total_transactions']}")
        print("=" * 50)
        
        return statement


# ========== 4. Extended Features (Bonus) ==========
class SavingsAccount(BankAccount):
    """Savings account with interest feature"""
    
    def __init__(self, account_holder: str, initial_balance: float = 0.0, interest_rate: float = 0.02):
        super().__init__(account_holder, initial_balance)
        self.interest_rate = interest_rate
    
    def add_interest(self) -> None:
        """Add interest to the account"""
        interest = self._balance * self.interest_rate
        print(f"\n💹 Adding interest: ${interest:,.2f} at {self.interest_rate*100}%")
        self.deposit(interest)
        # Override log to show as INTEREST
        self.transaction_history[-1]['transaction_type'] = 'INTEREST'


class Bank:
    """Bank system managing multiple accounts"""
    
    def __init__(self, name: str):
        self.name = name
        self.accounts: Dict[str, BankAccount] = {}
    
    def create_account(self, account_holder: str, initial_balance: float = 0.0, 
                      account_type: str = "checking") -> BankAccount:
        """Create a new account"""
        if account_type.lower() == "savings":
            account = SavingsAccount(account_holder, initial_balance)
        else:
            account = BankAccount(account_holder, initial_balance)
        
        self.accounts[account.account_number] = account
        return account
    
    def get_account(self, account_number: str) -> Optional[BankAccount]:
        """Retrieve account by number"""
        return self.accounts.get(account_number)
    
    def transfer(self, from_acc_num: str, to_acc_num: str, amount: float) -> bool:
        """Transfer money between accounts"""
        from_account = self.get_account(from_acc_num)
        to_account = self.get_account(to_acc_num)
        
        if not from_account or not to_account:
            print("❌ Invalid account number(s)")
            return False
        
        try:
            from_account.withdraw(amount)
            to_account.deposit(amount)
            print(f"\n✅ Transfer successful: ${amount:,.2f} from {from_acc_num} to {to_acc_num}")
            return True
        except (InsufficientFundsError, InvalidAmountError) as e:
            print(f"❌ Transfer failed: {e}")
            return False


# ========== 5. Example Usage and Testing ==========
def demo_banking_system():
    """Demonstrate the banking system functionality"""
    
    print("=" * 60)
    print("🏦 WELCOME TO THE BANKING SYSTEM DEMO")
    print("=" * 60)
    
    # Create accounts
    print("\n1️⃣ CREATING ACCOUNTS")
    print("-" * 40)
    account1 = BankAccount("John Doe", 1000.00)
    account2 = BankAccount("Jane Smith", 500.00)
    
    # Test deposits
    print("\n2️⃣ TESTING DEPOSITS")
    print("-" * 40)
    account1.deposit(500.00)
    account2.deposit(250.00)
    
    # Test withdrawals
    print("\n3️⃣ TESTING WITHDRAWALS")
    print("-" * 40)
    account1.withdraw(200.00)
    
    # Test error handling
    print("\n4️⃣ TESTING ERROR HANDLING")
    print("-" * 40)
    try:
        account1.withdraw(2000.00)  # Should fail
    except InsufficientFundsError as e:
        print(f"❌ Error caught: {e}")
    
    try:
        account1.deposit(-50.00)  # Should fail
    except InvalidAmountError as e:
        print(f"❌ Error caught: {e}")
    
    # Check balances
    print("\n5️⃣ CHECKING BALANCES")
    print("-" * 40)
    account1.get_balance()
    account2.get_balance()
    
    # View transaction history
    print("\n6️⃣ VIEWING TRANSACTION HISTORY")
    print("-" * 40)
    account1.get_transaction_history()
    
    # Generate statement
    print("\n7️⃣ GENERATING STATEMENT")
    print("-" * 40)
    account1.get_statement()
    
    # Test with Savings Account
    print("\n8️⃣ SAVINGS ACCOUNT DEMO")
    print("-" * 40)
    savings = SavingsAccount("Alice Johnson", 1000.00, interest_rate=0.05)
    savings.deposit(500.00)
    savings.withdraw(200.00)
    savings.add_interest()
    savings.get_balance()
    
    # Test Bank system with multiple accounts
    print("\n9️⃣ BANK SYSTEM WITH TRANSFERS")
    print("-" * 40)
    bank = Bank("Demo Bank")
    acc1 = bank.create_account("Bob Wilson", 1000.00)
    acc2 = bank.create_account("Carol Davis", 500.00, "savings")
    
    bank.transfer(acc1.account_number, acc2.account_number, 300.00)
    
    acc1.get_balance()
    acc2.get_balance()
    
    print("\n" + "=" * 60)
    print("✅ DEMO COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    demo_banking_system()