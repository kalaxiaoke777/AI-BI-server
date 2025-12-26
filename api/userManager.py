from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import JWTError, jwt
from typing import List, Dict, Any, Optional
from loguru import logger
from pydantic import BaseModel, EmailStr, Field
import hashlib

from db import get_db
from db.models import User, UserRole

# 创建路由
router = APIRouter()

# 密码加密上下文
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT配置
SECRET_KEY = "your-secret-key"  # 实际应用中应从配置文件获取
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


# 模型定义
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    email: EmailStr = Field(..., description="邮箱")
    password: str = Field(..., min_length=6, description="密码")
    role: UserRole = Field(default=UserRole.USER, description="用户角色")


class UserUpdate(BaseModel):
    username: Optional[str] = Field(
        None, min_length=3, max_length=50, description="用户名"
    )
    email: Optional[EmailStr] = Field(None, description="邮箱")
    password: Optional[str] = Field(None, min_length=6, description="密码")
    role: Optional[UserRole] = Field(None, description="用户角色")
    is_active: Optional[bool] = Field(None, description="是否激活")


class UserLogin(BaseModel):
    username: str = Field(..., description="用户名")
    password: str = Field(..., description="密码")


class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str
    user_info: UserResponse


# 工具函数
def _preprocess_password(password: str) -> str:
    """
    预处理密码,解决 bcrypt 72 字节限制问题
    使用 SHA256 将任意长度的密码转换为固定长度的哈希值
    """
    # 将密码转换为 SHA256 哈希(十六进制字符串,64字符,远小于72字节)
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(plain_password, hashed_password):
    """验证密码"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    """获取密码哈希"""
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """创建访问令牌"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# 依赖项
def get_current_user(db: Session = Depends(get_db), token: str = Query(...)):
    """获取当前用户"""
    credentials_exception = HTTPException(
        status_code=401,
        detail="无法验证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise credentials_exception

    if not user.is_active:
        raise HTTPException(status_code=400, detail="用户已被禁用")

    return user


def get_current_active_admin(current_user: User = Depends(get_current_user)):
    """获取当前活跃的管理员用户"""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(status_code=403, detail="无权限访问")
    return current_user


# 路由
@router.post("/login", response_model=Token, tags=["用户认证"])
async def login_for_access_token(login_data: UserLogin, db: Session = Depends(get_db)):
    """用户登录，获取访问令牌"""
    logger.info(f"用户登录请求，用户名: {login_data.username}")

    # 查找用户
    user = db.query(User).filter(User.username == login_data.username).first()
    if not user:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    # 验证密码
    if not verify_password(login_data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    # 检查用户是否激活
    if not user.is_active:
        raise HTTPException(status_code=400, detail="用户已被禁用")

    # 创建访问令牌
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user.id)}, expires_delta=access_token_expires
    )

    logger.info(f"用户登录成功，用户名: {login_data.username}")

    # 返回令牌和用户信息
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_info": UserResponse.from_orm(user),
    }


@router.post("/register", response_model=UserResponse, tags=["用户管理"])
async def register_user(user_data: UserCreate, db: Session = Depends(get_db)):
    """注册新用户"""
    logger.info(f"用户注册请求，用户名: {user_data.username}")

    # 检查用户名是否已存在
    existing_user = db.query(User).filter(User.username == user_data.username).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="用户名已存在")

    # 检查邮箱是否已存在
    existing_email = db.query(User).filter(User.email == user_data.email).first()
    if existing_email:
        raise HTTPException(status_code=400, detail="邮箱已存在")

    # 创建新用户
    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        username=user_data.username,
        email=user_data.email,
        password_hash=hashed_password,
        role=user_data.role,
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    logger.info(f"用户注册成功，用户名: {user_data.username}")

    return UserResponse.from_orm(new_user)


@router.get("/me", response_model=UserResponse, tags=["用户管理"])
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """获取当前用户信息"""
    return UserResponse.from_orm(current_user)


@router.get("/users", response_model=List[UserResponse], tags=["用户管理"])
async def get_all_users(
    skip: int = Query(0, ge=0, description="跳过记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db),
):
    """获取所有用户列表（仅管理员可访问）"""
    users = db.query(User).offset(skip).limit(limit).all()
    return [UserResponse.from_orm(user) for user in users]


@router.get("/users/{user_id}", response_model=UserResponse, tags=["用户管理"])
async def get_user(
    user_id: int,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db),
):
    """获取指定用户信息（仅管理员可访问）"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    return UserResponse.from_orm(user)


@router.put("/users/{user_id}", response_model=UserResponse, tags=["用户管理"])
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db),
):
    """更新用户信息（仅管理员可访问）"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 更新用户信息
    if user_data.username:
        # 检查用户名是否已存在
        existing_user = (
            db.query(User)
            .filter(User.username == user_data.username, User.id != user_id)
            .first()
        )
        if existing_user:
            raise HTTPException(status_code=400, detail="用户名已存在")
        user.username = user_data.username

    if user_data.email:
        # 检查邮箱是否已存在
        existing_email = (
            db.query(User)
            .filter(User.email == user_data.email, User.id != user_id)
            .first()
        )
        if existing_email:
            raise HTTPException(status_code=400, detail="邮箱已存在")
        user.email = user_data.email

    if user_data.password:
        user.password_hash = get_password_hash(user_data.password)

    if user_data.role is not None:
        # 检查是否试图将自己的角色从管理员降级
        if user_id == current_user.id and user_data.role != UserRole.ADMIN:
            raise HTTPException(status_code=400, detail="不能将自己的角色从管理员降级")
        user.role = user_data.role

    if user_data.is_active is not None:
        # 检查是否试图禁用自己的账号
        if user_id == current_user.id and user_data.is_active == False:
            raise HTTPException(status_code=400, detail="不能禁用自己的账号")
        user.is_active = user_data.is_active

    db.commit()
    db.refresh(user)

    logger.info(f"更新用户信息成功，用户ID: {user_id}")

    return UserResponse.from_orm(user)


@router.delete("/users/{user_id}", tags=["用户管理"])
async def delete_user(
    user_id: int,
    current_user: User = Depends(get_current_active_admin),
    db: Session = Depends(get_db),
):
    """删除用户（仅管理员可访问）"""
    # 检查是否试图删除自己
    if user_id == current_user.id:
        raise HTTPException(status_code=400, detail="不能删除自己的账号")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 软删除：将用户标记为非激活
    user.is_active = False
    db.commit()

    logger.info(f"删除用户成功，用户ID: {user_id}")

    return {"status": "success", "message": "用户已删除"}
