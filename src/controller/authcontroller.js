import { prisma } from "../lib/prisma.js";
import jwt from "jsonwebtoken";
import bcrypt from "bcrypt";
import { logger } from "../config/logger.js";
import { postgresOperationErrors } from "../config/metrics.js";
import { WelcomeEmailJob } from "../Broker/email.broker.js";

const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function createToken(userId) {
  if (!process.env.JWT_SECRET) throw new Error("JWT_SECRET is not configured");
  return jwt.sign({ id: userId }, process.env.JWT_SECRET, { expiresIn: "1h" });
}

export async function Register(req, res) {
  const { name, email, password } = req.body || {};
  if (
    typeof name !== "string" ||
    !name.trim() ||
    name.trim().length > 100 ||
    typeof email !== "string" ||
    !emailPattern.test(email) ||
    email.length > 255 ||
    typeof password !== "string" ||
    password.length < 8 ||
    password.length > 72
  ) {
    return res.status(400).json({
      success: false,
      message: "Provide a valid name, email, and password (8-72 characters).",
    });
  }

  try {
    const passwordHash = await bcrypt.hash(password, 12);
    const user = await prisma.user.create({
      data: { name: name.trim(), email: email.trim().toLowerCase(), passwordHash },
      select: { id: true, name: true, email: true },
    });

    try {
      await WelcomeEmailJob({
        to: user.email,
        subject: "Welcome to PlusePoll",
        body: `Welcome ${user.name}`,
      });
    } catch (error) {
      logger.warn({ err: error }, "Welcome email could not be queued");
    }

    return res.status(201).json({
      success: true,
      message: "User registered.",
      data: { ...user, token: createToken(user.id) },
    });
  } catch (error) {
    if (error?.code === "P2002") {
      return res.status(409).json({ success: false, message: "Email is already registered." });
    }
    postgresOperationErrors.inc();
    logger.error({ err: error }, "Registration failed");
    return res.status(500).json({ success: false, message: "Unable to register user." });
  }
}

export async function Login(req, res) {
  const { email, password } = req.body || {};
  if (
    typeof email !== "string" ||
    !emailPattern.test(email) ||
    typeof password !== "string" ||
    !password
  ) {
    return res.status(400).json({ success: false, message: "Provide a valid email and password." });
  }

  try {
    const user = await prisma.user.findUnique({
      where: { email: email.trim().toLowerCase() },
      select: { id: true, name: true, email: true, passwordHash: true },
    });
    if (!user || !(await bcrypt.compare(password, user.passwordHash))) {
      return res.status(401).json({ success: false, message: "Invalid email or password." });
    }
    return res.status(200).json({
      success: true,
      message: "Logged in.",
      data: {
        id: user.id,
        name: user.name,
        email: user.email,
        token: createToken(user.id),
      },
    });
  } catch (error) {
    postgresOperationErrors.inc();
    logger.error({ err: error }, "Login failed");
    return res.status(500).json({ success: false, message: "Unable to log in." });
  }
}

export async function resetPassword(_req, res) {
  return res.status(501).json({
    success: false,
    message: "Password reset is not available yet.",
  });
}

export async function PasswordReset(req, res) {
  return resetPassword(req, res);
}

export async function VerificationOPT(req, res) {
  return resetPassword(req, res);
}
