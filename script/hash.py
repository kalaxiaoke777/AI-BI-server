from passlib.context import CryptContext

# 创建一个 CryptContext 实例
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 哈希一个密码
hashed_password = pwd_context.hash("mysecretpassword")
print(f"Hashed Password: {hashed_password}")

# 验证密码
is_valid = pwd_context.verify("mysecretpassword", hashed_password)
print(f"Password is valid: {is_valid}")  # 输出: Password is valid: True

is_valid = pwd_context.verify("wrongpassword", hashed_password)
print(f"Password is valid: {is_valid}")  # 输出: Password is valid: False
