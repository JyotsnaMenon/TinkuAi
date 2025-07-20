import { users, campuses, events, chatSessions, type User, type InsertUser, type Campus, type InsertCampus, type Event, type InsertEvent, type ChatSession, type InsertChatSession } from "@shared/schema";

export interface IStorage {
  // User operations
  getUser(id: number): Promise<User | undefined>;
  getUserByEmail(email: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;
  
  // Campus operations
  getCampus(id: number): Promise<Campus | undefined>;
  getAllCampuses(): Promise<Campus[]>;
  createCampus(campus: InsertCampus): Promise<Campus>;
  updateCampus(id: number, updates: Partial<InsertCampus>): Promise<Campus | undefined>;
  
  // Event operations
  getEvent(id: number): Promise<Event | undefined>;
  getEventsByCampus(campusId: number): Promise<Event[]>;
  getEventsInDateRange(campusId: number, startDate: Date, endDate: Date): Promise<Event[]>;
  createEvent(event: InsertEvent): Promise<Event>;
  updateEvent(id: number, updates: Partial<InsertEvent>): Promise<Event | undefined>;
  deleteEvent(id: number): Promise<boolean>;
  
  // Chat operations
  createChatSession(chat: InsertChatSession): Promise<ChatSession>;
  getChatHistory(userId: number, limit?: number): Promise<ChatSession[]>;
  
  // Analytics
  getEventTypeDistribution(campusId: number): Promise<{ type: string; count: number }[]>;
  getMonthlyParticipation(campusId: number, year: number): Promise<{ month: string; participants: number }[]>;
  getTopRatedEvents(campusId: number, limit?: number): Promise<Event[]>;
}

export class MemStorage implements IStorage {
  private users: Map<number, User>;
  private campuses: Map<number, Campus>;
  private events: Map<number, Event>;
  private chatSessions: Map<number, ChatSession>;
  private currentUserId: number;
  private currentCampusId: number;
  private currentEventId: number;
  private currentChatId: number;

  constructor() {
    this.users = new Map();
    this.campuses = new Map();
    this.events = new Map();
    this.chatSessions = new Map();
    this.currentUserId = 1;
    this.currentCampusId = 1;
    this.currentEventId = 1;
    this.currentChatId = 1;
    
    // Initialize with sample data
    this.initializeData();
  }

  private initializeData() {
    // Create sample campus
    const campus: Campus = {
      id: 1,
      name: "Tech University Campus",
      description: "Empowering innovation through collaborative learning",
      logoUrl: null,
      bannerUrl: null,
      memberCount: 450,
      createdAt: new Date(),
    };
    this.campuses.set(1, campus);
    this.currentCampusId = 2;

    // Create sample user
    const user: User = {
      id: 1,
      email: "lead@techuniversity.edu",
      password: "password123",
      name: "John Doe",
      role: "campus_lead",
      campusId: 1,
      createdAt: new Date(),
    };
    this.users.set(1, user);
    this.currentUserId = 2;
  }

  async getUser(id: number): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(user => user.email === email);
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const id = this.currentUserId++;
    const user: User = { 
      ...insertUser, 
      id, 
      role: insertUser.role || "member",
      campusId: insertUser.campusId || null,
      createdAt: new Date() 
    };
    this.users.set(id, user);
    return user;
  }

  async getCampus(id: number): Promise<Campus | undefined> {
    return this.campuses.get(id);
  }

  async getAllCampuses(): Promise<Campus[]> {
    return Array.from(this.campuses.values());
  }

  async createCampus(insertCampus: InsertCampus): Promise<Campus> {
    const id = this.currentCampusId++;
    const campus: Campus = { 
      ...insertCampus, 
      id, 
      description: insertCampus.description || null,
      logoUrl: insertCampus.logoUrl || null,
      bannerUrl: insertCampus.bannerUrl || null,
      memberCount: insertCampus.memberCount || null,
      createdAt: new Date() 
    };
    this.campuses.set(id, campus);
    return campus;
  }

  async updateCampus(id: number, updates: Partial<InsertCampus>): Promise<Campus | undefined> {
    const campus = this.campuses.get(id);
    if (!campus) return undefined;
    
    const updatedCampus = { ...campus, ...updates };
    this.campuses.set(id, updatedCampus);
    return updatedCampus;
  }

  async getEvent(id: number): Promise<Event | undefined> {
    return this.events.get(id);
  }

  async getEventsByCampus(campusId: number): Promise<Event[]> {
    return Array.from(this.events.values())
      .filter(event => event.campusId === campusId)
      .sort((a, b) => new Date(b.dateTime).getTime() - new Date(a.dateTime).getTime());
  }

  async getEventsInDateRange(campusId: number, startDate: Date, endDate: Date): Promise<Event[]> {
    return Array.from(this.events.values())
      .filter(event => 
        event.campusId === campusId &&
        new Date(event.dateTime) >= startDate &&
        new Date(event.dateTime) <= endDate
      );
  }

  async createEvent(insertEvent: InsertEvent): Promise<Event> {
    const id = this.currentEventId++;
    const event: Event = {
      ...insertEvent,
      id,
      description: insertEvent.description || null,
      participantCount: insertEvent.participantCount || null,
      expense: insertEvent.expense || null,
      rating: insertEvent.rating || null,
      images: insertEvent.images || [],
      dateTime: new Date(insertEvent.dateTime),
      endDateTime: insertEvent.endDateTime ? new Date(insertEvent.endDateTime) : null,
      createdAt: new Date(),
    };
    this.events.set(id, event);
    return event;
  }

  async updateEvent(id: number, updates: Partial<InsertEvent>): Promise<Event | undefined> {
    const event = this.events.get(id);
    if (!event) return undefined;
    
    const updatedEvent = { 
      ...event, 
      ...updates,
      images: updates.images || event.images,
      dateTime: updates.dateTime ? new Date(updates.dateTime) : event.dateTime,
      endDateTime: updates.endDateTime ? new Date(updates.endDateTime) : event.endDateTime,
    };
    this.events.set(id, updatedEvent);
    return updatedEvent;
  }

  async deleteEvent(id: number): Promise<boolean> {
    return this.events.delete(id);
  }

  async createChatSession(insertChat: InsertChatSession): Promise<ChatSession> {
    const id = this.currentChatId++;
    const chat: ChatSession = { ...insertChat, id, createdAt: new Date() };
    this.chatSessions.set(id, chat);
    return chat;
  }

  async getChatHistory(userId: number, limit: number = 10): Promise<ChatSession[]> {
    return Array.from(this.chatSessions.values())
      .filter(chat => chat.userId === userId)
      .sort((a, b) => {
        const dateA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
        const dateB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
        return dateB - dateA;
      })
      .slice(0, limit);
  }

  async getEventTypeDistribution(campusId: number): Promise<{ type: string; count: number }[]> {
    const events = await this.getEventsByCampus(campusId);
    const distribution = events.reduce((acc, event) => {
      acc[event.programType] = (acc[event.programType] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);
    
    return Object.entries(distribution).map(([type, count]) => ({ type, count }));
  }

  async getMonthlyParticipation(campusId: number, year: number): Promise<{ month: string; participants: number }[]> {
    const events = await this.getEventsByCampus(campusId);
    const monthlyData = events
      .filter(event => new Date(event.dateTime).getFullYear() === year)
      .reduce((acc, event) => {
        const month = new Date(event.dateTime).toLocaleString('default', { month: 'long' });
        acc[month] = (acc[month] || 0) + (event.participantCount || 0);
        return acc;
      }, {} as Record<string, number>);
    
    return Object.entries(monthlyData).map(([month, participants]) => ({ month, participants }));
  }

  async getTopRatedEvents(campusId: number, limit: number = 5): Promise<Event[]> {
    const events = await this.getEventsByCampus(campusId);
    return events
      .sort((a, b) => (b.rating || 0) - (a.rating || 0))
      .slice(0, limit);
  }
}

// Use Supabase storage when DATABASE_URL is available, otherwise use in-memory
let storage: IStorage;

if (process.env.DATABASE_URL) {
  try {
    console.log('Attempting to use Supabase storage...');
    const { SupabaseStorage } = require('./storage-supabase');
    storage = new SupabaseStorage();
    console.log('✅ Successfully connected to Supabase!');
  } catch (error) {
    console.log('❌ Supabase storage not available, using in-memory storage');
    console.log('Error:', error instanceof Error ? error.message : String(error));
    storage = new MemStorage();
  }
} else {
  console.log('No DATABASE_URL found, using in-memory storage');
  storage = new MemStorage();
}

export { storage };
