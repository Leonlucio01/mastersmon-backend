import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

export function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

export function hashPassword(password) {
  return bcrypt.hash(String(password), 12);
}

export function comparePassword(password, passwordHash) {
  return bcrypt.compare(String(password), String(passwordHash || ""));
}

export function signToken(user) {
  const secret = process.env.JWT_SECRET;

  if (!secret) {
    const error = new Error("Missing JWT_SECRET environment variable.");
    error.status = 500;
    throw error;
  }

  return jwt.sign(
    {
      sub: user.id,
      email: user.email,
    },
    secret,
    {
      expiresIn: process.env.JWT_EXPIRES_IN || "7d",
    }
  );
}

export async function getUserFromToken(req, query) {
  const header = req.get("authorization") || "";
  const match = header.match(/^Bearer\s+(.+)$/i);

  if (!match) return null;

  const secret = process.env.JWT_SECRET;
  if (!secret) {
    const error = new Error("Missing JWT_SECRET environment variable.");
    error.status = 500;
    throw error;
  }

  let payload;
  try {
    payload = jwt.verify(match[1], secret);
  } catch (jwtError) {
    const error = new Error(jwtError?.name === "TokenExpiredError" ? "Token expired." : "Invalid token.");
    error.status = 401;
    error.code = jwtError?.name === "TokenExpiredError" ? "TOKEN_EXPIRED" : "TOKEN_INVALID";
    throw error;
  }

  const rows = await query(
    "SELECT id, email FROM game.users WHERE id = $1 AND is_active = true LIMIT 1",
    [payload.sub]
  );

  if (!rows.length) {
    const error = new Error("Authenticated user not found.");
    error.status = 401;
    error.code = "USER_NOT_FOUND";
    throw error;
  }

  return {
    id: rows[0].id,
    email: rows[0].email,
  };
}

export async function getCurrentUser(req, query) {
  const tokenUser = await getUserFromToken(req, query);
  if (tokenUser) return tokenUser;

  const fallbackEmail = normalizeEmail(process.env.CURRENT_USER_EMAIL);
  if (!fallbackEmail) {
    const error = new Error("Authentication required.");
    error.status = 401;
    error.code = "AUTH_REQUIRED";
    throw error;
  }

  // TODO: remove CURRENT_USER_EMAIL fallback after JWT login is fully enforced.
  const rows = await query(
    "SELECT id, email FROM game.users WHERE email = $1 AND is_active = true LIMIT 1",
    [fallbackEmail]
  );

  if (!rows.length) {
    const error = new Error("Fallback current user not found.");
    error.status = 401;
    error.code = "FALLBACK_USER_NOT_FOUND";
    throw error;
  }

  return {
    id: rows[0].id,
    email: rows[0].email,
  };
}
